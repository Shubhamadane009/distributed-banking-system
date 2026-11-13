# distributed-banking-system
A distributed banking application built using Spring Boot microservices, Apache Kafka, Redis, and Saga-based transaction processing with real-time fraud detection.

# Distributed Banking System with Real-Time Fraud Detection

A distributed banking application built using **Java, Spring Boot Microservices, Apache Kafka, Redis, and MySQL**. The system demonstrates event-driven communication, Saga-style transaction processing, distributed service interactions, and real-time fraud detection.

## Overview

The Distributed Banking System is designed to simulate core banking operations through independently deployable microservices.

It supports account management, fund transfers, transaction processing, fraud detection, payment integration, and notification event processing.

The application uses Apache Kafka for asynchronous communication between services and Redis for fraud detection, OTP management, and API rate limiting.

## Key Features

- **Microservices Architecture:** Independently structured Spring Boot services with dedicated responsibilities.
- **Event-Driven Communication:** Apache Kafka enables asynchronous communication between services.
- **Saga-Based Transaction Processing:** Coordinates distributed banking operations through events and compensating actions.
- **Fraud Detection:** Redis-powered transaction velocity checks, unusual transaction amount detection, and balance threshold checks.
- **OTP Verification:** Redis-based OTP storage and expiration for suspicious transactions.
- **API Gateway:** Centralized routing and Redis-based rate limiting.
- **Account Management:** Account creation, balance management, and account operations.
- **Payment Integration:** Razorpay integration for payment processing.
- **Notification Processing:** Kafka-based notification event consumption.

## System Architecture

The application consists of six Spring Boot services:

| Microservice | Responsibility |
|---|---|
| API Gateway | Centralized API routing and rate limiting |
| Account Service | Account management and balance operations |
| Transaction Service | Fund transfers and transaction lifecycle management |
| Fraud Detection Service | Transaction risk analysis and fraud checks |
| Payment Service | Payment processing integration |
| Notification Service | Notification event processing |

### Technology Stack

| Technology | Purpose |
|---|---|
| Java | Backend programming language |
| Spring Boot | Microservices development |
| Spring Cloud Gateway | API Gateway |
| Spring Cloud OpenFeign | Synchronous inter-service communication |
| Apache Kafka | Asynchronous event-driven communication |
| Redis | Fraud detection, OTP storage, and rate limiting |
| MySQL | Relational data persistence |
| Spring Data JPA | Database interaction |
| Docker Compose | Infrastructure setup |

## Transaction Processing

The system implements an event-driven Saga-style workflow for coordinating distributed banking transactions.

A typical fund transfer follows these steps:

1. A client initiates a fund transfer through the API Gateway.
2. Transaction Service processes the request and initiates the debit operation through Account Service.
3. Transaction details are persisted.
4. Transaction Service publishes an event to Apache Kafka.
5. Fraud Detection Service consumes the event and evaluates the transaction.
6. Depending on the fraud assessment, the workflow proceeds to verification or transaction completion.
7. For suspicious transactions, OTP verification is initiated.
8. Successful verification allows the transaction workflow to complete.
9. Failed or expired verification triggers a compensating refund operation.
10. Account and Notification services process the relevant transaction events.

## Redis-Based Fraud Detection

Redis is used to support multiple fraud detection mechanisms:

- Transaction velocity monitoring within a time window.
- Detection of unusually large transactions compared with the stored moving average.
- Transaction amount checks against available account balance thresholds.
- Temporary OTP storage with expiration.

These mechanisms demonstrate how Redis can support fast, stateful checks in a distributed application.

## Inter-Service Communication

The system uses two communication mechanisms:

**Synchronous Communication**
- Spring Cloud OpenFeign for direct service-to-service API calls.

**Asynchronous Communication**
- Apache Kafka for event publishing and consumption.
- Event-driven coordination between Transaction, Account, Fraud Detection, and Notification services.

## Project Structure

```text
banking-system/
├── account-service/
├── transaction-service/
├── fraud-detection-service/
├── payment-service/
├── notification-service/
├── api-gateway/
├── docker-compose.yml
└── README.md
```

## Running the Application

Prerequisites:
- Java JDK
- Maven
- Docker Desktop
- MySQL
- Redis

1. Clone the repository.
2. Configure the required database connections and service properties.
3. Start the infrastructure using Docker Compose.
4. Build and run each Spring Boot microservice.
5. Start API Gateway.
6. Access the application through the gateway endpoints.

Refer to individual service configurations for their ports and dependencies.

## Learning Objectives

This project explores practical backend engineering concepts:

- Designing Spring Boot microservices.
- Event-driven architecture with Apache Kafka.
- Saga pattern and compensating transactions.
- Redis for low-latency data operations.
- Synchronous and asynchronous service communication.
- Distributed transaction lifecycle management.
- API Gateway and rate limiting.

## Future Enhancements

- Transactional Outbox Pattern.
- Stronger idempotency and duplicate event protection.
- Improved concurrent balance update handling.
- Kafka retry and dead-letter queue strategies.
- Enhanced authentication and authorization.
- Expanded integration and failure-recovery testing.

---

**Project Type:** Backend / Distributed Systems  
**Primary Focus:** Java Backend Engineering and Microservices Architecture
