# Event-Driven E-Commerce Order Processing System

# Overview

This project simulates an E-Commerce Order Processing Platform built using Event-Driven Microservices Architecture with Spring Boot, Apache Kafka, PostgreSQL, and Docker.

The platform consists of multiple independent microservices responsible for order management, payment processing, inventory management, notifications, and analytics. Instead of communicating through direct service-to-service calls, the services exchange events asynchronously using Apache Kafka.

Apache Kafka acts as the central event streaming platform that enables services to communicate through events rather than synchronous API calls. This approach improves scalability, fault tolerance, service independence, and supports eventual consistency across distributed systems.

This project demonstrates how enterprise systems implement distributed business workflows using Kafka and several commonly used integration patterns, including:

- Saga Pattern (Choreography-Based Saga)
- Transactional Outbox Pattern
- Idempotent Consumer Pattern
- Retry & Dead Letter Topic (DLT) Pattern
- Eventual Consistency
- Event-Driven Analytics
- Shared Contracts Module
- Database-per-Service Pattern

Business Domain:

```text
Customer Places Order
        ↓
Order Service
        ↓
Payment Service
        ↓
Inventory Service
        ↓
Notification Service
        ↓
Analytics Service
```

# Architecture

```text
                    +------------------+
                    |  order-service   |
                    +------------------+
                             |
                             | order-created (Topic)
                             v

                    +------------------+
                    | payment-service  |
                    +------------------+
                             |
                             | payment-succeeded (Topic)
                             v

                    +------------------+
                    | inventory-service|
                    +------------------+
                      |            |
                      |            |
 inventory-reserved   |            | inventory-failed (Topic)
 (Topic)              |            |
                      v            v

            +----------------+   +----------------+
            | order-service  |   | payment-service|
            +----------------+   +----------------+
                  |                    |
              COMPLETED                |
                                       | payment-refunded (Topic)
                                       |
                                       v

                               +----------------+
                               | order-service  |
                               +----------------+
                                       |
                                   CANCELLED
```

# Microservices

## contracts

Shared library used by all microservices.

Responsibilities:

- Shared Event Objects
- Kafka Topic Definitions
- Common Event Contracts
- Prevent Event Duplication Across Services
- Ensure Consistent Event Structure

Examples:

- OrderCreatedEvent
- PaymentSucceededEvent
- InventoryReservedEvent
- InventoryFailedEvent
- PaymentRefundedEvent

Packaging: `contracts.jar`

Used By:

```text
order-service
payment-service
inventory-service
notification-service
analytics-service
```

## order-service

Responsibilities:

- Create Orders
- Complete Orders
- Cancel Orders
- Publish Order Events using Transactional Outbox Pattern
- Consume Inventory Reserved Events
- Consume Payment Refunded Events

Patterns Implemented:

- Choreography-Based Saga
- Transactional Outbox Pattern
- Eventual Consistency
- Database-per-Service Pattern

Database: `order_db`

## payment-service

Responsibilities:

- Process Payments
- Consume Order Created Events
- Publish Payment Success Events
- Consume Inventory Failed Events
- Publish Payment Refunded Events

Patterns Implemented:

- Choreography-Based Saga
- Eventual Consistency
- Database-per-Service Pattern

Database: `payment_db`

## inventory-service

Responsibilities:

- Consume Payment Success Events
- Reserve Inventory
- Handle Inventory Failures
- Publish Inventory Reserved Events
- Publish Inventory Failed Events
- Retry Processing
- Dead Letter Topic (DLT) Handling
- Idempotent Event Processing

Patterns Implemented:

- Choreography-Based Saga
- Idempotent Consumer Pattern
- Retry & Dead Letter Topic (DLT) Pattern
- Eventual Consistency
- Database-per-Service Pattern

Database: `inventory_db`

## notification-service

Responsibilities:

- Consume Inventory Reserved Events
- Consume Payment Refunded Events
- Send Order Completion Notifications
- Send Order Cancellation Notifications

Patterns Implemented:

- Event-Driven Communication

Database: `Not Required`

## analytics-service

Responsibilities:

- Consume Business Events
- Maintain Event-Driven Metrics
- Track Total Orders
- Track Completed Orders
- Track Cancelled Orders

Consumed Topics:

- order-created
- inventory-reserved
- payment-refunded

Patterns Implemented:

- Event-Driven Analytics
- Eventual Consistency
- Database-per-Service Pattern

Database: `analytics_db`


# Technology Stack

- Java 21
- Spring Boot 3.5
- Spring Kafka
- Spring Data JPA
- PostgreSQL
- Apache Kafka
- Docker
- Docker Compose
- Lombok
- Swagger / OpenAPI
- Maven


# Kafka Topics

| Topic | Purpose |
|---------|---------|
| order-created | Order created event |
| payment-succeeded | Payment completed |
| inventory-reserved | Inventory reserved |
| inventory-failed | Inventory unavailable |
| payment-refunded | Payment refunded |
| payment-succeeded-retry | Retry topic |
| payment-succeeded-dlt | Dead letter topic |


# How to run the Project

### Step 1: Start Kafka Infrastructure

Start Kafka and Kafka UI using Docker.
* `docker-compose up -d`

Verify containers are running:
* `docker ps`

**Concept:** Kafka acts as the event broker between microservices.

<img width="2384" height="378" alt="image" src="https://github.com/user-attachments/assets/0ddde89e-3f33-4812-8efe-3d33b9c6f51c"/>
### Step 2: Create Kafka Topics

Open Kafka UI and create the following topics: `http://localhost:8080`
* order-created
* payment-succeeded
* inventory-reserved
* inventory-failed
* payment-refunded
* payment-succeeded-retry
* payment-succeeded-dlt

Topic Configuration:
* `Partitions: 3`
* `Replication Factor: 1`

What do you mean by Topic, Partition and Replication Factor:

**Topics:** Topics are communication channels used by services to exchange events.

**Partitions:** A topic is divided into partitions. Kafka distributes messages across partitions, allowing multiple consumers to process messages in parallel and improving scalability.

**Replication Factor:** Defines how many copies of a partition Kafka maintains across brokers for fault tolerance. A replication factor of 1 means no backup copy exists. For production environments, a replication factor of 3 is commonly used to ensure high availability if a broker fails.

**Why 3 Partitions?** This project uses 3 partitions to demonstrate Kafka's partitioning and parallel processing capabilities. Since the setup runs on a single Kafka broker locally, a replication factor of 1 is sufficient for development and learning purposes. <br />

<img width="3576" height="1484" alt="image" src="https://github.com/user-attachments/assets/9f4495bf-1761-48f6-b53a-1ae933cff5de" /> <br />

<img width="3578" height="1294" alt="image" src="https://github.com/user-attachments/assets/3c122bde-7421-4f11-85aa-e450fc8d13d2" />

### Step 3: Clone Project

* `git clone https://github.com/Prathmesh717/event-driven-ecommerce-microservices.git`
* `cd event-driven-ecommerce-microservices`

**Concept:** This project follows Event-Driven Microservices Architecture.

### Step 4: Create Databases

Create the following PostgreSQL databases:

* `CREATE DATABASE order_db;`
* `CREATE DATABASE payment_db;`
* `CREATE DATABASE inventory_db;`
* `CREATE DATABASE analytics_db;`

**Concept:** Database-per-Service pattern. Each microservice owns its own database.

**Create Tables and Seed Data:** Execute SQL scripts provided in each service.

* order-service: `order_db.sql`
    * It creates following tables:
        * `orders`
        * `outbox_events`

* payment-service: `payment_db.sql`
    * It creates following table:
        * `payments`

* inventory-service: `inventory_db.sql`
    * It creates following tables and seeds sample inventory data:
        * `inventory`
        * `processed_events`

* analytics-service: `analytics_db.sql`
    * It creates following table and seeds initial metrics:
        * `order_metrics`

**Concept:** Outbox Pattern and Idempotent Consumer Pattern require dedicated tables.

<img width="3584" height="2240" alt="image" src="https://github.com/user-attachments/assets/f86b8d17-2196-4151-b75a-e646b2939052" />

### Step 5: Build Entire Project & Start Services

Build all modules from root project: `mvn clean install`

**Concept:** Multi-Module Maven Build compiles all services and shared contracts together.

Start services in the following order: `mvn spring-boot:run`
1. order-service
2. payment-service
3. inventory-service
4. notification-service
5. analytics-service

**Concept:** Services communicate asynchronously through Kafka events.

<img width="3584" height="2100" alt="image" src="https://github.com/user-attachments/assets/684e0187-af07-4eb6-8032-319acbc911f5" />

### Step 6: Swagger UI
* `http://localhost:8081/swagger-ui/index.html`
* `http://localhost:8085/swagger-ui/index.html`


# Positive Scenario Test

### Create Order

Request:

```http
POST http://localhost:8081/api/v1/orders
```

```json
{
  "customerId": 101,
  "productId": 1001,
  "quantity": 2,
  "amount": 499.99
}
```
<img width="3582" height="2088" alt="image" src="https://github.com/user-attachments/assets/6623b6be-f4fc-411c-95ae-fd8fe1f5fab2" /> <br />

Expected Flow

```text
Order Created
      ↓
Payment Success
      ↓
Inventory Reserved
      ↓
Order Completed
```
<img width="3576" height="1316" alt="image" src="https://github.com/user-attachments/assets/dd8c1463-a23a-4175-9c8a-4990876cd2dd" /> <br />

<img width="3584" height="1584" alt="image" src="https://github.com/user-attachments/assets/93f10833-9335-4af5-a2cb-70fdfe653316" /> <br />

<img width="3584" height="1628" alt="image" src="https://github.com/user-attachments/assets/6cb3ba62-9bd4-4b0a-a3fa-6bc45809f9ee" /> <br />

<img width="3584" height="1476" alt="image" src="https://github.com/user-attachments/assets/2d8c1793-ea12-4ac4-a28a-80a393138fd3" /> <br />

**Verify Order Status**

```sql
SELECT * FROM orders;
```

Expected Result: `status = COMPLETED`

<img width="2292" height="486" alt="image" src="https://github.com/user-attachments/assets/0b729a57-4705-4dc9-a206-dcc2dd56dffb" /> <br />

**Verify Payment Status**

```sql
SELECT * FROM payments;
```

Payment amount and status is saved.

<img width="2302" height="516" alt="image" src="https://github.com/user-attachments/assets/b823b8c6-9122-4c85-998f-0d8dc5d695f5" /> <br />

**Verify Inventory Status**

```sql
SELECT * FROM inventory WHERE product_id = 1001;
```

Inventory quantity should be reduced.

<img width="2294" height="724" alt="image" src="https://github.com/user-attachments/assets/71778a6c-f808-4572-aa52-c0c7e7dd7bcd" /> <br />

**Verify Analytics**

```http
GET http://localhost:8085/api/v1/analytics/metrics
```

Expected:

```json
{
  "totalOrders": 1,
  "completedOrders": 1,
  "cancelledOrders": 0
}
```

<img width="3582" height="1574" alt="image" src="https://github.com/user-attachments/assets/75c29d03-8287-44af-bedd-f368a57dd8be" />

**Concept:** Saga Pattern successful execution path.


# Negative Scenario Test

Use a product with no inventory.

Example:

```json
{
  "customerId": 101,
  "productId": 1007,
  "quantity": 1,
  "amount": 499.99
}
```

<img width="3584" height="2062" alt="image" src="https://github.com/user-attachments/assets/0e87b753-52a5-4909-838f-7d2032e722b8" /> <br />

Expected Flow

```text
Order Created
      ↓
Payment Success
      ↓
Inventory Failed
      ↓
Payment Refunded
      ↓
Order Cancelled
```

<img width="3584" height="1332" alt="image" src="https://github.com/user-attachments/assets/85854a48-ea34-47ab-af80-3b43668cd191" /> <br />

<img width="3584" height="1682" alt="image" src="https://github.com/user-attachments/assets/55a10e90-e9b2-48d5-ae1c-a4ed1b014aac" /> <br />

<img width="3584" height="1716" alt="image" src="https://github.com/user-attachments/assets/4285858f-9995-49da-8c6b-6f7b17f449b9" /> <br />

<img width="3582" height="1514" alt="image" src="https://github.com/user-attachments/assets/84797677-71e3-4888-84a3-796539c63b88" /> <br />

<img width="3584" height="1486" alt="image" src="https://github.com/user-attachments/assets/2c670af3-4f10-4c1a-a469-8eaf48881845" /> <br />

**Verify Order Status**

```sql
SELECT * FROM orders;
```

Expected: `status = CANCELLED`

<img width="2574" height="556" alt="image" src="https://github.com/user-attachments/assets/c7b53718-61a5-485c-abc6-6637c002efee" /> <br />

**Verify Payment Status**

```sql
SELECT * FROM payments;
```

Expected: `status = REFUNDED`

<img width="2574" height="566" alt="image" src="https://github.com/user-attachments/assets/0bfb76b0-ac7c-467c-855a-38e17a29354c" /> <br />

**Verify Analytics**

```http
GET http://localhost:8085/api/v1/analytics/metrics
```

Expected:

```json
{
  "totalOrders": 2,
  "completedOrders": 1,
  "cancelledOrders": 1
}
```

<img width="3584" height="1574" alt="image" src="https://github.com/user-attachments/assets/0cad2bf1-67b8-4e0d-b1d8-7b6b7956890d" />

**Concept:** Saga Pattern compensation flow.


# Retry & Dead Letter Topic (DLT) Test

**What is Retry?**

Retry is a mechanism that automatically attempts to process a failed event again because some failures may be temporary (network issue, database issue, downstream service unavailable, etc.).

**What is a Dead Letter Topic (DLT)?**

If an event continues to fail even after multiple retry attempts, it is moved to a dedicated Dead Letter Topic (DLT) for manual investigation instead of blocking normal message processing.

**Where is it Implemented?**

This project implements Retry and DLT handling in:

```text
inventory-service
```

The Inventory Service retries failed `PaymentSucceededEvent` messages before sending them to the Dead Letter Topic.

**Test Scenario**

This project intentionally treats product ID `9999` as a failure scenario.

When Inventory Service receives:

```text
productId = 9999 (this ID not exist in the table)
```

it throws an exception to simulate a processing failure.

Create an order using:

```json
{
  "customerId": 101,
  "productId": 9999,
  "quantity": 1,
  "amount": 100
}
```

**Expected Flow**

```text
Order Service
    ↓
order-created (Kafka Topic)
    ↓
Payment Service
    ↓
payment-succeeded (Kafka Topic)
    ↓
Inventory Service
    ↓
Exception Occurred (Its because no productID=9999 exist in the inventory table)
    ↓
payment-succeeded-retry (Kafka Topic)
    ↓
Retry Attempt #1
    ↓
Exception Occurred
    ↓
payment-succeeded-retry (Kafka Topic)
    ↓
Retry Attempt #2
    ↓
Exception Occurred
    ↓
payment-succeeded-dlt (Kafka Topic)
    ↓
DLT Consumer
    ↓
Error Logged for Manual Investigation
```

**Verify in Kafka UI**

Verify messages are published to:

```text
payment-succeeded-retry
payment-succeeded-dlt
```

**Expected Result**

```text
Original event processing fails
    ↓
Event is retried multiple times
    ↓
Event is moved to DLT
    ↓
Application continues processing other messages normally
```

**Concept:** Retry Processing and Dead Letter Topic (DLT) handling improve system resilience by isolating permanently failed events from normal business processing.

<img width="3582" height="2052" alt="image" src="https://github.com/user-attachments/assets/cb2773c7-3c08-4754-a14b-449ba969d32d" /> <br />

<img width="3584" height="1342" alt="image" src="https://github.com/user-attachments/assets/daf011c9-2f8b-49ae-b294-958355075992" /> <br />

<img width="3582" height="2142" alt="image" src="https://github.com/user-attachments/assets/d00ffd03-87d2-4281-a0cf-da11034c1d29" /> <br />

<img width="3578" height="1642" alt="image" src="https://github.com/user-attachments/assets/7a89c323-5566-4976-a355-c75aeba819bf" />

# Outbox Pattern Verification

**What is the Outbox Pattern?**

The Outbox Pattern ensures that database updates and event publishing remain consistent.

Without the Outbox Pattern, the following problem can occur:

```text
Save Order in Database
        ↓
Kafka Publish Fails
```

In this scenario, the order is created in the database, but other services never receive the event, resulting in an inconsistent system state.

To solve this, the application first stores the event in an Outbox table within the same database transaction as the business data. A separate publisher then reads the Outbox table and publishes the event to Kafka.

**Where is it Implemented?**

This project implements the Transactional Outbox Pattern in: `order-service`

Database Table: `order_db.outbox_events`

**How It Works**

```text
Create Order API
        ↓
Save Order in order_db.orders
        ↓
Save OrderCreatedEvent in order_db.outbox_events
        ↓
Commit Transaction
        ↓
Outbox Publisher
        ↓
Publish OrderCreatedEvent
        ↓
order-created (Kafka Topic)
        ↓
Mark Event as Published
```

**Verification**

Create a new order.

Verify:`1SELECT * FROM outbox_events;`

Expected: `published = true`

This confirms that:

```text
Order was saved successfully
        ↓
Event was stored in Outbox table
        ↓
Outbox Publisher published event to Kafka
        ↓
Event marked as published
```

**Concept:** Transactional Outbox Pattern prevents database and Kafka inconsistency by ensuring events are never lost after a successful database transaction.

<img width="2832" height="480" alt="image" src="https://github.com/user-attachments/assets/9b19a1c9-0210-4279-9648-8734f5f2cef3" />

# Idempotent Consumer Verification

**What is the Idempotent Consumer Pattern?**

Kafka guarantees at-least-once delivery, which means the same message may be delivered more than once.

Without an Idempotent Consumer, duplicate messages could result in the same business operation being executed multiple times.

Example:

```text
PaymentSucceededEvent
        ↓
Inventory Reserved
        ↓
Kafka Re-delivers Same Event
        ↓
Inventory Reserved Again
```

This could incorrectly reduce inventory multiple times for the same order.

To solve this, the consumer keeps track of already processed events and ignores duplicates.

**Where is it Implemented?**

This project implements the Idempotent Consumer Pattern in: `inventory-service`

Database Table: `inventory_db.processed_events`

**How It Works**

```text
PaymentSucceededEvent
        ↓
payment-succeeded (Kafka Topic)
        ↓
Inventory Service
        ↓
Check processed_events table
        ↓
Event Already Processed?
        ↓
YES
        ↓
Ignore Event
```

```text
PaymentSucceededEvent
        ↓
payment-succeeded (Kafka Topic)
        ↓
Inventory Service
        ↓
Check processed_events table
        ↓
Event Already Processed?
        ↓
NO
        ↓
Reserve Inventory
        ↓
Save Event Id in processed_events
        ↓
Publish InventoryReservedEvent
        ↓
inventory-reserved (Kafka Topic)
```

**Verification**

Verify: `SELECT * FROM processed_events;`

Expected: `One record per processed event.`

**Expected Result**

```text
Each event is processed only once
        ↓
Duplicate Kafka messages are ignored
        ↓
Inventory is not reserved multiple times
```

**Concept:** The Idempotent Consumer Pattern prevents duplicate processing by recording processed event IDs and ignoring repeated Kafka messages.

<img width="2826" height="400" alt="image" src="https://github.com/user-attachments/assets/8f52d29b-742f-488d-b49d-094c2aa54b8e" />

# Saga Pattern Verification

**What is the Saga Pattern?**

The Saga Pattern is used to manage distributed transactions across multiple microservices without using a single shared database transaction.

Instead of a traditional distributed transaction, each service performs a local transaction and publishes an event. Other services react to the event and continue the business process.

If a step fails, compensating actions are executed to undo previously completed operations.

**Where is it Implemented?**

This project implements the Saga Pattern across:

```text
order-service
payment-service
inventory-service
notification-service
analytics-service
```

#### Successful Order Flow (called Happy Path)

**Test Payload**

```json
{
  "customerId": 101,
  "productId": 1001,
  "quantity": 2,
  "amount": 499.99
}
```

Reason:

```text
productId = 1001
available_quantity = 100

requested_quantity = 2

100 >= 2
```

Inventory can be reserved successfully.

**Flow**

```text
Client
        ↓
POST /api/v1/orders
        ↓
Order Service
        ↓
Save Order
        ↓
order-created (Kafka Topic)
        ↓
Payment Service
        ↓
Process Payment
        ↓
payment-succeeded (Kafka Topic)
        ↓
Inventory Service
        ↓
Reserve Inventory
        ↓
inventory-reserved (Kafka Topic)
        ↓
Order Service
        ↓
Order Status = COMPLETED
```

#### Compensation Flow (called Failure Path)

When inventory is unavailable, the Saga executes compensating actions.

**Test Payload**

```json
{
  "customerId": 101,
  "productId": 1007,
  "quantity": 1,
  "amount": 499.99
}
```

Reason:

```text
productId = 1007
available_quantity = 0

requested_quantity = 1

0 < 1
```

Inventory cannot be reserved.

**Flow**

```text
Client
        ↓
POST /api/v1/orders
        ↓
Order Service
        ↓
order-created (Kafka Topic)
        ↓
Payment Service
        ↓
payment-succeeded (Kafka Topic)
        ↓
Inventory Service
        ↓
Inventory Not Available
        ↓
inventory-failed (Kafka Topic)
        ↓
Payment Service
        ↓
Refund Payment
        ↓
payment-refunded (Kafka Topic)
        ↓
Order Service
        ↓
Order Status = CANCELLED
```

**Verification**

Successful Order Verification: `SELECT status FROM orders;`
* Expected: `COMPLETED`

Successful Payment Verification: `SELECT status FROM payments;`
* Expected: `SUCCESS`

Failed Order Verification: `SELECT status FROM orders;`
* Expected: `CANCELLED`

Failed Payment Verification: `SELECT status FROM payments;`
* Expected: `REFUNDED`

<img width="2552" height="588" alt="image" src="https://github.com/user-attachments/assets/102b727d-0f91-4c18-90bb-cb4a379705f4" /> <br />

<img width="2552" height="608" alt="image" src="https://github.com/user-attachments/assets/a79ee080-5610-48a4-86b5-8ac576e0208d" /> <br />

**Expected Result**

```text
Each service owns its own database
        ↓
Services communicate through Kafka events
        ↓
Business transaction spans multiple services
        ↓
Failures trigger compensating actions
        ↓
System remains eventually consistent
```

**Concept:** The Saga Pattern manages distributed business transactions using events and compensating actions while maintaining eventual consistency across microservices.


# Event-Driven Analytics Verification

**What is Event-Driven Analytics?**

Event-Driven Analytics allows business metrics and reports to be generated by consuming events from Kafka instead of querying operational databases directly.

This keeps business services independent and prevents reporting workloads from impacting transactional systems.

**Where is it Implemented?**

This project implements Event-Driven Analytics in: `analytics-service`

Database Table: `analytics_db.order_metrics`

**Events Consumed**

The Analytics Service consumes the following events:

```text
order-created (Kafka Topic)

inventory-reserved (Kafka Topic)

payment-refunded (Kafka Topic)
```

**How It Works**

When an order is created:

```text
Order Service
        ↓
order-created (Kafka Topic)
        ↓
Analytics Service
        ↓
Increment TOTAL_ORDERS
```

When an order completes successfully:

```text
Inventory Service
        ↓
inventory-reserved (Kafka Topic)
        ↓
Analytics Service
        ↓
Increment COMPLETED_ORDERS
```

When an order is cancelled:

```text
Payment Service
        ↓
payment-refunded (Kafka Topic)
        ↓
Analytics Service
        ↓
Increment CANCELLED_ORDERS
```

**Example: Successful Order**

Request:

```json
{
  "customerId": 101,
  "productId": 1001,
  "quantity": 2,
  "amount": 499.99
}
```

Analytics Update:

```text
TOTAL_ORDERS      +1
COMPLETED_ORDERS  +1
```

Verify:

```http
GET http://localhost:8085/api/v1/analytics/metrics
```

Expected Response:

```json
{
  "totalOrders": 1,
  "completedOrders": 1,
  "cancelledOrders": 0
}
```

**Example: Cancelled Order**

Request:

```json
{
  "customerId": 101,
  "productId": 1007,
  "quantity": 1,
  "amount": 499.99
}
```

Analytics Update:

```text
TOTAL_ORDERS      +1
CANCELLED_ORDERS  +1
```

Verify:

```http
GET http://localhost:8085/api/v1/analytics/metrics
```

Expected Response:

```json
{
  "totalOrders": 2,
  "completedOrders": 1,
  "cancelledOrders": 1
}
```

**Verification**

Verify directly from database:

```sql
SELECT *
FROM order_metrics;
```

Example:

```text
metric_name         metric_value
--------------------------------
TOTAL_ORDERS        2
COMPLETED_ORDERS    1
CANCELLED_ORDERS    1
```

**Expected Result**

```text
Business events are consumed from Kafka
        ↓
Metrics are updated asynchronously
        ↓
No direct database dependency between services
        ↓
Reporting is separated from transactional workloads
```

**Concept:** Event-Driven Analytics enables real-time reporting by consuming business events and updating analytics data independently of operational services.

<img width="2828" height="586" alt="image" src="https://github.com/user-attachments/assets/112890a6-1efb-4e14-8fe8-50f85650f374" /> <br />

<img width="2556" height="596" alt="image" src="https://github.com/user-attachments/assets/3569ccbc-646c-4fec-83d8-141ffd69894d" /> <br />

<img width="3584" height="1572" alt="image" src="https://github.com/user-attachments/assets/c372d241-c759-4f25-9bf8-ddb19c7fc8a6" />

# Summary

This project demonstrates how enterprise systems implement:

- Event-Driven Architecture
- Apache Kafka Messaging
- Choreography-Based Saga Pattern
- Eventual Consistency
- Transactional Outbox Pattern
- Idempotent Consumer Pattern
- Retry & Dead Letter Topic (DLT) Pattern
- Event-Driven Analytics
- Shared Contracts Module
- Database-per-Service Pattern
- Microservices with Spring Boot

## License

This project is licensed under the Apache License, Version 2.0.

You may obtain a copy of the License at:

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

See the [LICENSE](LICENSE) file for the specific language governing permissions and limitations under the License.
