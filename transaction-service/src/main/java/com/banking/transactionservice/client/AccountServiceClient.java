package com.banking.transactionservice.client;

import org.apache.commons.lang3.ClassUtils;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

// Declares a Feign client to communicate with the Account Service.
@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    // Sends an HTTP PUT request to deduct money from an account.
    // {accountNumber} is replaced with the actual account number.

    //it's a Java method declaration in a feign interface, not a String. String is its return type.
    @PutMapping("/api/v1/accounts/{accountNumber}/deduct")
    String deductBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount
            );

    @PutMapping("/api/v1/accounts/{accountNumber}/credit")
    String creditBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount);

}
