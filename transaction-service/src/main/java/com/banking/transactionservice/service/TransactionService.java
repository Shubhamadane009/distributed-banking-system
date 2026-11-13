package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    //private final TransactionService transactionService;
    private final TransactionRepository transactionRepository;
    private  final AccountServiceClient accountServiceClient;
    //private final TransactionInitiatedEvent transactionInitiatedEvent;

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    private final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private final String FRAUD_DETECTED_TOPIC = "fraud.detected";
    /*
    *SAGA step1 : Initiate transfer
    * Deducts money from sender via feign
    * Saves transaction as PROCESSING.
    * public event to kafka for fraud check
    * Returns
     */

    public TransactionResponse transfer(@Valid TransferRequest request) {
        log.info("SAGA start - Transfer: {} -> {} amount: {}"
                ,request.getSenderAccountNumber(),
                request.getAmount());

        //SAGA step1 deduct from sender
        //called an abstract method -> deductBalance from AccountServiceClient.java interface present in account-service
        //goto that method and read comments you'll get the idea of everything
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        //here we saved the transaction
        log.info("Transaction saved as PROCESSING: {}",savedTransaction.getId());

        //next to publish initiated event to kafka
        //Its SAGA step 2 : publish event for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(),event);
        log.info("SAGA step 2 - TransactionInitiatedEvent published {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
        //here our transfer method is implemented
    }

    private TransactionResponse mapToResponse(Transaction transaction){

        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setAmount(transaction.getAmount());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setCompletedAt(transaction.getCompletedAt());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setDescription(transaction.getDescription());
        response.setFailureReason(transaction.getFailureReason());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setStatus(transaction.getStatus());
        response.setType(transaction.getType());

        return response;
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found: "+transactionId)));

    }
    public List<TransactionResponse> getTransactionHistory(String userAccountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(userAccountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for the transaction: {}",transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found "+transactionId));

        String otpKey = "verification:otp"+transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            //otp expired
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction,"OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            //BLOCK ACCOUNT AND REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);

            blockAccountAndCompensate(transaction,
                    "Wrong OTP entered - transaction cancelled, "+
                    "account blocked for security");

            return mapToResponse(transaction);

        }

        //OTP correct - complete the transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason){
        log.warn("SAGA COMPENSATION  - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        //CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                "- SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);

        //publis an event to - Notification service will alert the user

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason",reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}",
                transaction.getAmount(),transaction.getSenderAccountNumber());


    }

    private void blockAccountAndCompensate(Transaction transaction, String reason){

        //publish fraud.detected -> Account service will blobk account
        Map<String,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getId(),fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked, kindly contact to the bank",
                transaction.getSenderAccountNumber());

        //SAGA Compensation -> refund sender
        compensateTransaction(transaction,reason);

        //account service client is already consuming the fraud.detected event and firing blockAccount method
        //so no futher things in this method
    }

    private void completeTransaction(Transaction transaction){
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        //now publishing trnsaction.completed event

        TransactionCompletedEvent transactionCompletedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),transactionCompletedEvent);
        log.info("SAGA COMPLETE - transaction {} completed",
                transaction.getId());
    }

    public void processCleanResult(String transactionId) {

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found "+transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction: {} not processing - skipping",transactionId);
            return;
        }

        completeTransaction(transaction);



    }
}
