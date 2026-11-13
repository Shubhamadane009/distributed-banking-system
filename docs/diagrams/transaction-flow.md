# Banking Transaction Flow – Saga Pattern

This diagram illustrates the end-to-end fund transfer workflow, including asynchronous fraud detection, OTP verification, transaction completion, and compensating actions.

## 1. Successful Transaction Flow

```mermaid
flowchart TD
    A["Client initiates fund transfer"] --> B["API Gateway"]
    B --> C["Transaction Service"]

    C --> D["Account Service via OpenFeign"]
    D --> E["Debit sender account"]

    E --> F["Save transaction as PROCESSING"]
    F --> G["Publish transaction.initiated"]

    G --> H[("Apache Kafka")]
    H --> I["Fraud Detection Service"]

    I --> J["Fetch sender balance via OpenFeign"]
    J --> K["Perform fraud checks using Redis"]

    K --> L{"Suspicious activity?"}

    L -->|No| M["Publish fraud.check.clean"]
    M --> N["Transaction Service consumes event"]
    N --> O["Update status to COMPLETED"]
    O --> P["Publish transaction.completed"]

    P --> Q["Account Service"]
    P --> R["Notification Service"]

    Q --> S["Credit receiver account"]
    R --> T["Generate debit and credit alerts"]
```

## 2. Suspicious Transaction – OTP Verification

```mermaid
flowchart TD
    A["Fraud Detection Service"] --> B["Publish verification.required"]
    B --> C[("Apache Kafka")]

    C --> D["Transaction Service consumes event"]
    D --> E["Generate OTP"]
    E --> F["Store OTP in Redis"]
    F --> G["Set 5-minute expiration"]

    G --> H["Update status to PENDING_VERIFICATION"]
    H --> I["Publish transaction.otp.generated"]

    I --> J["Notification Service"]
    J --> K["Log OTP notification"]

    K --> L["User submits OTP"]
    L --> M["Transaction Service verifies OTP"]

    M --> N{"OTP valid and not expired?"}

    N -->|Yes| O["Complete transaction"]
    O --> P["Publish transaction.completed"]
    P --> Q["Account Service credits receiver"]

    N -->|No / Expired| R["Execute compensation"]
    R --> S["Refund sender through Account Service"]
    S --> T["Update transaction status to FLAGGED"]
    T --> U["Publish transaction.refunded"]
```

## 3. Failed Verification – Account Blocking

```mermaid
flowchart TD
    A["User submits incorrect OTP"] --> B["Transaction Service"]
    B --> C["Publish fraud.detected"]
    C --> D[("Apache Kafka")]

    D --> E["Account Service consumes event"]
    E --> F["Block sender account"]

    B --> G["Saga Compensation"]
    G --> H["Credit refunded amount to sender"]
    H --> I["Update transaction status"]
    I --> J["Publish transaction.refunded"]

    D --> K["Notification Service"]
    J --> K
    K --> L["Log security and refund notifications"]
```

## Saga Workflow Summary

| Stage | Action | Service |
|---|---|---|
| 1 | Initiate transfer | Transaction Service |
| 2 | Debit sender | Account Service |
| 3 | Publish transaction event | Transaction Service |
| 4 | Evaluate transaction risk | Fraud Detection Service |
| 5 | Request OTP if suspicious | Fraud Detection Service |
| 6 | Generate and store OTP | Transaction Service + Redis |
| 7 | Complete or compensate | Transaction Service |
| 8 | Credit receiver or refund sender | Account Service |
| 9 | Process notifications | Notification Service |

**Key Design Concepts**

- Event-driven communication using Apache Kafka.
- Saga-style distributed transaction processing.
- Redis-based fraud detection and temporary OTP storage.
- Synchronous inter-service communication using OpenFeign.
- Compensating transactions for failed verification.
- Asynchronous account updates and notification processing.
