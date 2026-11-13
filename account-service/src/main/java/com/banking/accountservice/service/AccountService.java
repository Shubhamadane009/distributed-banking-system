package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.respository.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();


    public AccountResponse createAccount(@Valid CreateAccountRequest request) {
        log.info("Creating account for: {}",request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exists for email: "+request.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generatAccountNumber());
        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.SAVINGS
                        ? new BigDecimal("10000")
                        : new BigDecimal("50000")
        );

        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse  response = new AccountResponse();

        response.setAccountHolderName(account.getAccountHolderName());
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountStatus(account.getAccountStatus());
        response.setAccountType(account.getAccountType());
        response.setBalance(account.getBalance());
        response.setCreatedAt(account.getCreatedAt());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());

        return response;
    }

    //generate unique 12 digit account number
    private String generatAccountNumber(){
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);

            accountNumber = String.format("%012d", number);// it will convert "1234" to "000000001234"
        }while(accountRepository.existsByAccountNumber(accountNumber));

            return accountNumber;
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        return account.getBalance();
    }

    /*
    * Block account - called by fraud detection service via kafka
    * @Param  accountNumber
     */

    public void blockAccount(String accountNumber) {
        log.info("Blocking acccount: {}",accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Account blocked: {}", accountNumber);


    }

    /*
    *deduct balance from sender.
    * Its called by transaction service
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting the balance: {}",accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        if(account.getAccountStatus()!= AccountStatus.ACTIVE){
            throw new RuntimeException("Account is  not active: {}"+accountNumber);
        }

        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient funds: {}"+accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("Balance updated. New balance is: {}",account.getBalance());

    }

    /*
    * credit balance and called by transaction service via kafka
     */
    public void creditBalance(String accountNumber, BigDecimal amount) {

        log.info("Crediting amount: {} to account: {}"+amount,accountNumber);


        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not found"));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        log.info("Amount credited. New balance is: {}",account.getBalance());

    }
}
