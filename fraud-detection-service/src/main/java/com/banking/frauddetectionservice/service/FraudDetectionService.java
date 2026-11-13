package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {


    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    private static final String VERIFICATION_REEQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransacationsPerMinute;//its value will be fetched from application.yml it is 5 there

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;//its value will be fetched from application.yml it is 5 there

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;//its value will be fetched from application.yml it is 0.90 there


    public void checkTransaction(Map<String, Object> payload) {
            String transactionId = (String)payload.get("transactionId");
            String accountNumber = (String)payload.get("senderAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            //fetch real balance from account-service
            //so lets go to AccountServiceClient

            //fetched account balance from the senders account
            BigDecimal senderAccountBalance = accountServiceClient.getBalance(accountNumber);

            log.info("Checking transaction: {} account: {} amount: {} balance: {}",
                    transactionId,accountNumber,amount,senderAccountBalance);

            FraudCheckResult result = performFraudChecks(accountNumber, amount, senderAccountBalance);

            if(result.isFraud()){
                log.info("Suspicious activity detected in account: {}"+
                            "reason: {} - Requesting OTP verification",
                        accountNumber,result.getReason());

                //so here we have noticed a suspicious activity
                //so lets verify with the user, that its him/her or some hacker
                //Then lets verify this , hence lets publish an kafka event for verification

                Map<String,Object> verificationEvent = new HashMap<>();
                verificationEvent.put("transactionId",transactionId);
                verificationEvent.put("accountNumber",accountNumber);
                verificationEvent.put("amount",amount);
                verificationEvent.put("reason",result.getReason());

                kafkaTemplate.send(VERIFICATION_REEQUIRED_TOPIC,transactionId,verificationEvent);
            }
            else{
                //if no fraud detected
                log.info("Transaction clean");

                Map<String,Object> transactionCleanEvent = new HashMap<>();
                transactionCleanEvent.put("transactionId",transactionId);
                transactionCleanEvent.put("isFraud",false);
                transactionCleanEvent.put("reason",null);

                //published an event that the transaction is clean as there is no fraud in it
                kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC,transactionId,transactionCleanEvent);
            }
    }

    private FraudCheckResult performFraudChecks(String accountNumber,
                                                BigDecimal amount,
                                                BigDecimal senderAccountBalance){
        //pattern 1 : velocity check
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true,"Too many transactions in 60 seconds "+
                    " - Velocity limit exceeded");
        }

        //pattern 2: balance check
        if(isAmountSuspicious(accountNumber, amount)){
            return new FraudCheckResult(true,"Unusual trasaction amount "+
                    " - exceedes 3x your avergae ");
        }

        //pattern 3: Balance check
        if(senderAccountBalance.compareTo(BigDecimal.ZERO) > 0
            && isBalanceCheckFailed(senderAccountBalance, amount)){
            return new FraudCheckResult(true,"Transaction exceeds 90% of account balance");
        }

        return new FraudCheckResult(false,null);
    }

    private boolean isVelocityExceeded(String accountNumber){

        String key = "fraud:velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1){
            redisTemplate.expire(key,60, TimeUnit.SECONDS);
        } //redis will remove that key for that specific accountNumber after 1 minute.
        //till then it will keep increamenting the count for that key for 1 minute

        log.info("Velocity check - account: {} count: {}/{}",
                accountNumber,count,maxTransacationsPerMinute);

        return count != null && count > maxTransacationsPerMinute;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount){
        String avgKey = "fraud:avg_number" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null){
            redisTemplate.opsForValue().set(avgKey,amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2),2, RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        log.info("Amount check - amount: {} threshold: {} suspicious: {}",
                amount, threshold, amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderAccountBalance, BigDecimal amount){
        BigDecimal maxAllowed = senderAccountBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage)
        );

        log.info("Balance check - amount: {} maxAlloed: {} suspicious: {}",
                amount,maxAllowed,amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }
}
