package com.banking.transactionservice.entity;

/*
* Transaction life-cycle flow
*
*
* Pending->Processing->Completed (clean transaction)
*                   ->Pending_verification (suspicious detected)
*                   ->Completed (verified)
*                   ->Flagged (SAGA refund)
*        ->Failed
*        ->Flagged
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
