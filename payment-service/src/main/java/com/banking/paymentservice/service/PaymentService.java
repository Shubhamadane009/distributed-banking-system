package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.banking.paymentservice.entity.Payment;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;


    /*
    *Create razorpay payment order
    *
    * Flow:
    * 1.Create order in razorpay
    * 2.save payment record in db
    * 3.return order details to frontend
    * 4.frontend show razorpay checkout
    * 5.user pays
    * 6.razorpay calls webhook
     */
    public PaymentOrderResponse createPaymentOrder(@Valid CreatePaymentRequest request) throws RazorpayException {

        //1.Creating  order in razorpay
        log.info("Create payment order for account: {} amount: {}",
                request.getAccountNumber(),request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount",convertedAmount);
        orderRequest.put("currency","USD");

        //our receipt id shoule be unique and should not exceed 30 characters
        orderRequest.put("receipt","rcpt_"+ System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("-","").substring(0, 10)
        );

        Order razorPayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorPayOrder.get("id").toString());

        //Now 2.Saving payment /order in db
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorPayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        //3.return order details
        return new PaymentOrderResponse(
          savedPayment.getRazorPayPaymentId(),
          razorPayOrder.get("id").toString(),
          request.getAmount(),
          "USD",
          "CREATED",
          keyId
        );



    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received RazorPay webhook: {}",payload.get("event"));

        String event = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }

    }

    private void handlePaymentSuccess(Map<String,Object> payload){
        try {
            Map<String,Object> paymentData = extractPaymentData(payload);
            String paymentId = (String) paymentData.get("id");
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: {}"+orderId
                    ));

            payment.setRazorPayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);

            paymentRepository.save(payment);

            //publish payment completed event to kafka
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(),event);
            log.info("Payment completed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error handling payment success",e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String,Object> payload){
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: {}"+orderId
                    ));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            //publish payment completed event to kafka
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("reason","Payment failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);
            log.warn("Payment failed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error handling payment failure: {}",e.getMessage());
        }
    }

    private Map<String,Object> extractPaymentData(Map<String,Object> payload){
        Map<String,Object> entity = (Map<String,Object>) payload.get("payload");

        Map<String,Object> paymentWrapper = (Map<String,Object>) entity.get("payment");

        return (Map<String,Object>) paymentWrapper.get("entity");
    }
}
