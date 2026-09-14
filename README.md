# 📬 Notification Service

Lightweight notification processing and delivery microservice designed as a core component of the distributed **Event-Driven Microservices Network**.

The service operates as a **Hybrid NestJS Application** providing:
1. A synchronous **HTTP REST API** for direct notification requests and health monitoring.
2. An asynchronous task queue worker powered by **Redis** (`BRPOP` in-memory queue).
3. A robust, production-grade **RabbitMQ (AMQP 0-9-1)** event consumer featuring manual acknowledgments (`ACK`/`NACK`), backpressure handling, and **Dead Letter Queue (DLQ)** routing for fault-tolerant message processing.
4. A high-throughput **Apache Kafka (KRaft)** distributed event streaming integration with order-preserving partition keys and consumer group scalability.

---

## 📑 Table of Contents

- [System Architecture](#-system-architecture)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Environment Variables](#-environment-variables)
- [API Endpoints](#-api-endpoints)
- [Apache Kafka Event Streaming (KRaft)](#-apache-kafka-event-streaming-kraft)
- [RabbitMQ Event Consumer (AMQP)](#-rabbitmq-event-consumer-amqp)
- [Asynchronous Redis Worker](#-asynchronous-redis-worker)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Local Development](#local-development)
  - [Docker Compose Deployment](#docker-compose-deployment)
- [Project Context & Tasks](#-project-context--tasks)

---

## 🏛 System Architecture

The service operates within an isolated Docker bridge network `microservices_net` alongside other platform services and message brokers:

```mermaid
flowchart TD
    subgraph Docker Network: microservices_net
        OrderService["order-service (Port 3000)<br/>NestJS + PostgreSQL"]
        AnalyticsService["analytics-service (Port 3001)<br/>NestJS + MongoDB"]
        NotificationService["notification-service (Port 3002)<br/>Hybrid NestJS (HTTP + RMQ + Kafka)"]
        RedisStore[("Redis (Port 6379)<br/>In-Memory Queue")]
        RabbitBroker[("RabbitMQ Broker (Port 5672 / 15672)<br/>AMQP 0-9-1 & Management UI")]
        KafkaBroker[("Kafka Broker KRaft (Port 9092 / 29092)<br/>Distributed Commit Log")]
        KafkaUI["Kafka UI (Port 8080)<br/>Cluster Management Web UI"]

        OrderService -- "Synchronous HTTP (REST)" --> AnalyticsService
        OrderService -. "Synchronous Fallback (HTTP)" .-> NotificationService
        
        OrderService -- "LPUSH queue:notifications (Async Task)" --> RedisStore
        RedisStore -- "BRPOP queue:notifications" --> NotificationService

        OrderService -- "Publish: 'order_created'" --> RabbitBroker
        RabbitBroker -- "Push: orders_queue" --> NotificationService
        
        NotificationService -. "NACK (requeue=false)" .-> RabbitBroker
        RabbitBroker -. "DLX: orders.dlx" .-> DLQ[("DLQ: orders.dead_letter")]

        NotificationService -- "Emit: 'order.status-changed' (Key: orderId)" --> KafkaBroker
        KafkaBroker -- "Pull: notification-kafka-group (3 Partitions)" --> NotificationService
        KafkaUI -- "Monitor Metrics (Port 29092)" --> KafkaBroker
    end
```

---

## 🚀 Key Features

1. **Synchronous Notification Processing (HTTP REST):**
   - Direct HTTP endpoint for receiving notification requests (`POST /notifications/send`).
   - Designed for baseline benchmarking and demonstrating cascading failure behavior when downstream dependencies fail.

2. **Distributed Event Streaming (Apache Kafka & KRaft):**
   - **ZooKeeper-less KRaft Mode:** High-performance metadata quorum (`apache/kafka:3.7.0`).
   - **Dual Listeners:** Configured for seamless hybrid execution (`INTERNAL://kafka:29092` in Docker and `EXTERNAL://localhost:9092` on host).
   - **Partition Key Ordering:** Guarantees strict chronological processing (`CREATED` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`) by hashing `orderId` via Murmur2 into designated topic partitions.
   - **Consumer Groups:** Scales processing across 3 partitions with `notification-kafka-group`.
   - **Web UI Management:** Real-time visibility into topics, offsets, and consumer lag at `http://localhost:8080`.

3. **Enterprise Message Broker Integration (RabbitMQ & AMQP 0-9-1):**
   - Built with `@nestjs/microservices`, `amqplib`, and `amqp-connection-manager`.
   - **Manual Acknowledgments (`noAck: false`):** Guaranteed *At-least-once delivery*; messages are acknowledged (`channel.ack()`) only upon successful business processing.
   - **Dead Letter Queue (DLQ):** Non-retryable or malformed payloads (*Poison Messages*) are rejected (`channel.nack(msg, false, false)`) and automatically routed to `orders.dlx` ➡️ `orders.dead_letter` for engineer auditing.
   - **Management Dashboard:** Real-time visibility into queue depth, ack rates, and exchange bindings at `http://localhost:15672`.

4. **Asynchronous Task Buffering (Redis Worker):**
   - **Producer-Consumer / Task Queue** pattern utilizing Redis `LPUSH` and `BRPOP`.
   - Protects system throughput during traffic spikes (*Spike Arresting*).

---

## 🛠 Tech Stack

- **Runtime:** Node.js (v20+) / TypeScript (Strict Mode)
- **Framework:** NestJS 12 (Hybrid HTTP + RabbitMQ + Kafka Application)
- **Event Streaming:** Apache Kafka 3.7.0 (KRaft mode) + Kafka UI
- **Message Broker:** RabbitMQ 3.13 (AMQP 0-9-1) with Management Web UI
- **Microservices Libraries:** `@nestjs/microservices`, `kafkajs`, `amqplib`, `amqp-connection-manager`
- **Cache & Task Queue:** Redis 7 (`ioredis`)
- **Containerization:** Docker & Docker Compose
- **Network:** Docker Bridge Network (`microservices_net`)

---

## ⚙️ Environment Variables

Create a `.env` file in the root directory based on the following template:

```env
# Application Port
PORT=3002
NOTIFICATION_SERVICE_PORT=3002

# Redis Configuration
# Use "redis" when running inside Docker Compose, "localhost" when running locally
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
NOTIFICATION_QUEUE_NAME=queue:notifications

# RabbitMQ Configuration
# Use "amqp://guest:guest@rabbitmq:5672" in Docker Compose, "amqp://guest:guest@localhost:5672" when running locally
RABBITMQ_URL=amqp://guest:guest@localhost:5672

# Kafka Configuration
# Use "kafka:29092" inside Docker Compose, "localhost:9092" when running locally
KAFKA_BROKER=localhost:9092

# Environment
NODE_ENV=development
```

---

## 📡 API Endpoints

### 1. Healthcheck

- **Method:** `GET`
- **Path:** `/health`
- **Response (`200 OK`):**
  ```json
  {
    "status": "ok",
    "uptime": 124.5,
    "timestamp": "2026-09-14T12:00:00.000Z"
  }
  ```

### 2. Synchronous Notification Dispatch

- **Method:** `POST`
- **Path:** `/notifications/send`
- **Headers:** `Content-Type: application/json`
- **Request Body:**
  ```json
  {
    "orderId": "ord-12345",
    "customerEmail": "customer@example.com",
    "message": "Your order has been placed successfully!"
  }
  ```
- **Response (`201 Created`):**
  ```json
  {
    "status": "SENT",
    "orderId": "ord-12345",
    "sentAt": "2026-09-14T12:00:00.000Z"
  }
  ```

### 3. Kafka Test Order Flow (Order Lifecycle Dispatch)

- **Method:** `POST`
- **Path:** `/notifications/test-flow/:orderId`
- **Path Parameter:** `:orderId` (e.g. `order-101`)
- **Description:** Emits 3 sequential events (`CREATED` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`) to topic `order.status-changed` using `orderId` as Partition Key.
- **Response (`201 Created`):**
  ```json
  {
    "message": "Events for order order-101 successfully sent",
    "statuses": [
      "CREATED",
      "PAID",
      "SHIPPED"
    ]
  }
  ```

---

## ⚡ Apache Kafka Event Streaming (KRaft)

The service integrates both a Kafka Producer and a Consumer:

- **Topic Name:** `order.status-changed` (3 partitions, replication factor 1)
- **Consumer Group:** `notification-kafka-group`
- **Message Pattern:** `@MessagePattern('order.status-changed')`
- **Guaranteed Order via Partition Key:**
  ```typescript
  // Producer transmits orderId as key to ensure all status changes land in the same partition:
  this.kafkaClient.emit('order.status-changed', {
    key: orderId,
    value: { orderId, status, payload, timestamp: new Date().toISOString() },
  });
  ```
- **Consumer Context Auditing:**
  The consumer extracts `context.getPartition()`, `rawMsg.offset`, and `rawMsg.key?.toString()` to verify partition affinity and strict sequence.

---

## 🐰 RabbitMQ Event Consumer (AMQP)

The microservice consumer connects to the `orders_queue` and subscribes to events:

- **Event Pattern:** `order_created`
- **Queue Name:** `orders_queue` (Durable, DLX configured)
- **Dead Letter Exchange:** `orders.dlx` (Routing key: `orders.dead_letter`)
- **Payload Structure (NestJS RMQ):**
  ```json
  {
    "pattern": "order_created",
    "data": {
      "orderId": 101,
      "customerEmail": "customer@example.com",
      "totalPrice": 450
    }
  }
  ```

### Lifecycle & Acknowledgment Flow:
1. **Valid Data:** `data.customerEmail` contains a valid address ➡️ simulated email dispatch ➡️ `channel.ack(originalMsg)`.
2. **Invalid Data (Poison Message):** Missing/invalid email ➡️ validation exception ➡️ `channel.nack(originalMsg, false, false)` ➡️ message routed to `orders.dead_letter`.

---

## 📥 Asynchronous Redis Worker

The legacy task worker initiates on application startup and performs a non-busy blocking wait for tasks using `BRPOP`:

- **Queue Name:** `queue:notifications`
- **Command:** `BRPOP queue:notifications 0`
- **Task Payload:**
  ```json
  {
    "orderId": "ord-12345",
    "customerEmail": "customer@example.com",
    "message": "Your order was created!"
  }
  ```

---

## 🏁 Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/) (v20 or higher)
- [Docker](https://www.docker.com/) & Docker Compose

### Local Development

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Start Infrastructure Services (Kafka, Kafka UI, RabbitMQ & Redis):**
   ```bash
   docker compose -f docker-compose.microservices.yml up -d kafka kafka-ui rabbitmq redis
   ```
   - Kafka UI Dashboard: [http://localhost:8080](http://localhost:8080).
   - RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672) (User: `guest` / Pass: `guest`).

3. **Run the Development Server:**
   ```bash
   npm run start:dev
   ```
   - HTTP API available at `http://localhost:3002`.
   - Kafka Consumer listening on `order.status-changed`.
   - AMQP Consumer listening to `orders_queue`.

### Docker Compose Deployment

To run the entire microservices topology inside Docker:

```bash
docker compose -f docker-compose.microservices.yml up -d --build notification-service kafka kafka-ui rabbitmq redis
```

Stream logs:
```bash
docker compose -f docker-compose.microservices.yml logs -f notification-service
```

---

## 📚 Project Context & Tasks

Detailed task specifications, architectural analyses, and interview preparation notes:
- [docs/task-1.md](docs/task-1.md) — *Day 10: Event-Driven — Docker Compose Network & Redis Buffer*.
- [docs/task-1-summary.md](docs/task-1-summary.md) — *Summary 1: Redis In-Memory Architecture, Lessons Learned & Scaling*.
- [docs/task-2.md](docs/task-2.md) — *Day 11: Event-Driven — Message Queues with RabbitMQ (AMQP 0-9-1)*.
- [docs/task-2-summary.md](docs/task-2-summary.md) — *Summary 2: AMQP Deep Dive, Manual ACK/NACK, DLQ & Technical Interview Q&A*.
- [docs/task-3.md](docs/task-3.md) — *Day 12: Event-Driven — Apache Kafka, Topics, Partitions & Consumer Groups*.
- [docs/task-3-summary.md](docs/task-3-summary.md) — *Summary 3: Kafka KRaft Deep Dive, Dual Listeners, Partition Key Guarantees & Interview Q&A*.
- [docs/postman/notification-service.postman_collection.json](docs/postman/notification-service.postman_collection.json) — *Postman Collection for API & Flow Testing*.

