# 📬 Notification Service

Lightweight notification processing and delivery microservice designed as a core component of the distributed **Event-Driven Microservices Network**.

The service operates as a **Hybrid NestJS Application** providing:
1. A synchronous **HTTP REST API** for direct notification requests, order creation, and health monitoring.
2. A resilient **Transactional Outbox Pattern** backed by **PostgreSQL 16 & TypeORM**, eliminating the **Dual-Write Problem** with atomic ACID transactions.
3. A concurrent **Outbox Polling Worker** utilizing PostgreSQL **`FOR UPDATE SKIP LOCKED`** for horizontal multi-instance scaling and reliable event publishing to Kafka.
4. An **Idempotent Consumer** featuring deterministic recursive **SHA-256** payload fingerprinting and TTL-based deduplication to guarantee *Effectively Exactly-Once* processing.
5. A high-throughput **Apache Kafka (KRaft)** distributed event streaming integration with order-preserving partition keys and consumer group scalability.
6. A robust, production-grade **RabbitMQ (AMQP 0-9-1)** event consumer featuring manual acknowledgments (`ACK`/`NACK`), backpressure handling, and **Dead Letter Queue (DLQ)** routing.
7. An asynchronous task queue worker powered by **Redis** (`BRPOP` in-memory queue).

---

## 📑 Table of Contents

- [System Architecture](#-system-architecture)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Environment Variables](#-environment-variables)
- [API Endpoints](#-api-endpoints)
- [Transactional Outbox & SKIP LOCKED Concurrency](#-transactional-outbox--skip-locked-concurrency)
- [Idempotent Consumer (SHA-256 Deduplication)](#-idempotent-consumer-sha-256-deduplication)
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

The service operates within an isolated Docker bridge network `microservices_net` alongside platform databases and message brokers:

```mermaid
flowchart TD
    subgraph Docker Network: microservices_net
        PostgresDB[("PostgreSQL 16 (Port 5433 / 5432)<br/>Tables: orders, outbox_events")]
        KafkaBroker[("Kafka Broker KRaft (Port 9092 / 29092)<br/>Topic: order.status-changed (3 Partitions)")]
        KafkaUI["Kafka UI (Port 8080)<br/>Cluster Management Web UI"]
        RabbitBroker[("RabbitMQ Broker (Port 5672 / 15672)<br/>AMQP 0-9-1 & Management UI")]
        RedisStore[("Redis (Port 6379)<br/>In-Memory Queue")]

        subgraph Notification Service (Port 3002)
            OrdersCtrl["OrdersController<br/>POST /orders"]
            OrdersSvc["OrdersService<br/>Atomic Transaction (ACID)"]
            OutboxWorker["OutboxProcessorWorker<br/>CRON Polling (SKIP LOCKED)"]
            KafkaConsumer["Kafka Consumer<br/>@MessagePattern('order.status-changed')"]
            IdempStore["IdempotencyStore<br/>Deterministic SHA-256 + TTL"]
            NotifSvc["NotificationsService<br/>Dispatch Notifications"]
            RMQConsumer["RabbitMQ Consumer<br/>Manual ACK / NACK / DLQ"]
            RedisWrk["Redis Task Worker<br/>BRPOP queue:notifications"]
        end

        OrdersCtrl --> OrdersSvc
        OrdersSvc -- "1. Save Order + 2. Save OutboxEvent (PENDING)" --> PostgresDB

        OutboxWorker -- "SELECT ... FOR UPDATE SKIP LOCKED" --> PostgresDB
        OutboxWorker -- "Emit: 'order.status-changed' (Key: orderId)" --> KafkaBroker
        OutboxWorker -- "UPDATE outbox_events (status='PUBLISHED')" --> PostgresDB

        KafkaBroker -- "Pull: notification-kafka-group" --> KafkaConsumer
        KafkaConsumer --> IdempStore
        IdempStore -- "New Event" --> NotifSvc
        IdempStore -. "Duplicate (Ignore)" .-> KafkaConsumer

        RabbitBroker -- "Push: orders_queue" --> RMQConsumer
        RMQConsumer -. "NACK (requeue=false)" .-> RabbitBroker
        RabbitBroker -. "DLX: orders.dlx" .-> DLQ[("DLQ: orders.dead_letter")]

        RedisStore -- "BRPOP queue:notifications" --> RedisWrk
        KafkaUI -- "Monitor Cluster" --> KafkaBroker
    end
```

---

## 🚀 Key Features

1. **Transactional Outbox Pattern (Dual-Write Resolution):**
   - Eliminates distributed state inconsistency between database and message brokers.
   - Saves business entity (`Order`) and event intent (`OutboxEvent` in status `PENDING`) within a single ACID transaction in PostgreSQL.
   - Zero data loss even during complete broker outage or network partition.

2. **Concurrent Outbox Polling Worker with `SKIP LOCKED`:**
   - Background CRON worker (`@nestjs/schedule`) polls PostgreSQL every 2 seconds.
   - Uses PostgreSQL **`FOR UPDATE SKIP LOCKED`** to safely scale across multiple replicas without duplicate message delivery or database lock contention.
   - Batching (`take(20)`) and chronological ordering prevent memory exhaustion and Thundering Herd spikes.
   - Retry logic tracks failures with `retryCount` and moves poison events to `FAILED` after 5 attempts.

3. **Idempotent Consumer (Deduplication):**
   - Bridges the gap from *At-least-once* broker delivery to *Effectively Exactly-Once* application processing.
   - Implements recursive key-sorted serialization and **SHA-256** hashing (`IdempotencyStore`) with configurable TTL caching.
   - Automatically detects and skips duplicate Kafka events without repeating business side-effects.

4. **Distributed Event Streaming (Apache Kafka & KRaft):**
   - **ZooKeeper-less KRaft Mode:** High-performance metadata quorum (`apache/kafka:3.7.0`).
   - **Dual Listeners:** Seamless execution between host (`localhost:9092`) and Docker network (`kafka:29092`).
   - **Partition Key Ordering:** Strict chronological order preserved per `orderId` via Murmur2 partition hashing.
   - **Kafka UI:** Real-time visibility into topics, partitions, and offsets at `http://localhost:8080`.

5. **Enterprise Message Broker Integration (RabbitMQ & AMQP 0-9-1):**
   - Manual acknowledgments (`channel.ack()`) guarantee no messages are lost in transit.
   - Non-retryable or poison payloads are rejected (`channel.nack()`) and routed to Dead Letter Queue (`orders.dead_letter`).
   - Real-time management console at `http://localhost:15672`.

6. **Asynchronous Task Buffering (Redis Worker):**
   - Non-busy task queue worker leveraging Redis `BRPOP` for spike arresting.

---

## 🛠 Tech Stack

- **Runtime:** Node.js (v20+) / TypeScript (Strict Mode, ESM)
- **Framework:** NestJS 12 (Hybrid HTTP + RabbitMQ + Kafka Application)
- **Database & ORM:** PostgreSQL 16 Alpine + TypeORM (`@nestjs/typeorm`, `typeorm`, `pg`)
- **Event Streaming:** Apache Kafka 3.7.0 (KRaft mode) + Kafka UI (`kafkajs`)
- **Message Broker:** RabbitMQ 3.13 (AMQP 0-9-1) with Management UI (`amqplib`)
- **In-Memory Cache & Queue:** Redis 7 (`ioredis`)
- **Job Scheduler:** `@nestjs/schedule` (CRON engine)
- **Containerization & Networking:** Docker & Docker Compose (`microservices_net`)

---

## ⚙️ Environment Variables

Create a `.env` file in the root directory based on the following template:

```env
# Application Port
PORT=3002
NOTIFICATION_SERVICE_PORT=3002

# PostgreSQL Database (Host port 5433 mapped to container 5432)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=order_db

# Kafka Configuration
# Use "kafka:29092" inside Docker Compose, "localhost:9092" when running locally
KAFKA_BROKER=localhost:9092

# RabbitMQ Configuration
# Use "amqp://guest:guest@rabbitmq:5672" in Docker Compose, "amqp://guest:guest@localhost:5672" when running locally
RABBITMQ_URL=amqp://guest:guest@localhost:5672

# Redis Configuration
# Use "redis" when running inside Docker Compose, "localhost" when running locally
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
NOTIFICATION_QUEUE_NAME=queue:notifications

# Environment
NODE_ENV=development
```

---

## 📡 API Endpoints

### 1. Create Order (Transactional Outbox)

- **Method:** `POST`
- **Path:** `/orders`
- **Headers:** `Content-Type: application/json`
- **Request Body:**
  ```json
  {
    "customerEmail": "student@example.com",
    "totalPrice": 1250.00
  }
  ```
- **Response (`201 Created`):**
  ```json
  {
    "id": "b3c69f2e-4b68-45e3-982f-2d85b14f6e10",
    "customerEmail": "student@example.com",
    "totalPrice": 1250,
    "status": "CREATED",
    "createdAt": "2026-09-15T09:30:00.000Z"
  }
  ```

### 2. Healthcheck

- **Method:** `GET`
- **Path:** `/health`
- **Response (`200 OK`):**
  ```json
  {
    "status": "ok",
    "uptime": 124.5,
    "timestamp": "2026-09-15T12:00:00.000Z"
  }
  ```

### 3. Synchronous Notification Dispatch

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

### 4. Kafka Test Order Flow

- **Method:** `POST`
- **Path:** `/notifications/test-flow/:orderId`
- **Description:** Emits 3 sequential events (`CREATED` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`) to Kafka topic `order.status-changed`.

---

## 🔄 Transactional Outbox & SKIP LOCKED Concurrency

### Database Schema

- **Table `orders`:** Stores primary domain entity (`id`, `customerEmail`, `totalPrice`, `status`, `createdAt`).
- **Table `outbox_events`:** Append-only event queue within PostgreSQL:
  - `id`: UUID (Primary Key)
  - `aggregateType`: `'Order'`
  - `aggregateId`: Order UUID
  - `eventType`: `'ORDER_CREATED'`
  - `payload`: JSONB event payload
  - `status`: Enum (`PENDING`, `PROCESSING`, `PUBLISHED`, `FAILED`) with B-Tree Index
  - `retryCount`: Integer failure counter
  - `errorMessage`: Diagnostic string
  - `createdAt` / `processedAt`: Audit timestamps

### Polling Worker Mechanism

```typescript
const pendingEvents = await manager
  .createQueryBuilder(OutboxEvent, 'event')
  .setLock('pessimistic_write') // FOR UPDATE
  .setOnLocked('skip_locked')   // SKIP LOCKED
  .where('event.status = :status', { status: OutboxStatus.PENDING })
  .orderBy('event.createdAt', 'ASC')
  .take(20)
  .getMany();
```

- If 3 worker replicas query PostgreSQL concurrently, **Worker 1** locks rows 1–20. **Worker 2** immediately skips rows 1–20 and processes rows 21–40 without blocking or generating duplicate publishes.

---

## 🛡️ Idempotent Consumer (SHA-256 Deduplication)

To defend against duplicate delivery inherent to *At-least-once* messaging:
1. The consumer receives an event from Kafka (`order.status-changed`).
2. `IdempotencyStore.generateHash()` sorts keys recursively and hashes the payload with SHA-256:
   $$\text{Hash} = \text{SHA256}(\text{CanonicalJSON}(\{ \text{key}, \text{payload} \}))$$
3. If the hash exists in the cache, the message is skipped with a duplicate warning.
4. If new, the hash is cached with a 2-minute TTL and business processing proceeds.

---

## ⚡ Apache Kafka Event Streaming (KRaft)

- **Topic:** `order.status-changed` (3 partitions, replication factor 1)
- **Consumer Group:** `notification-kafka-group`
- **Partition Key:** `orderId` ensures all lifecycle events for a single order land on the exact same partition, preserving strict chronological processing order.

---

## 🐰 RabbitMQ Event Consumer (AMQP)

- **Queue Name:** `orders_queue` (Durable, DLX configured)
- **Dead Letter Exchange:** `orders.dlx` (Routing key: `orders.dead_letter`)
- **Pattern:** `@EventPattern('order_created')`
- **Manual Ack:** Acknowledged via `channel.ack(originalMsg)`. Invalid emails rejected via `channel.nack(originalMsg, false, false)` and routed to DLQ.

---

## 📥 Asynchronous Redis Worker

- **Queue Name:** `queue:notifications`
- **Command:** `BRPOP queue:notifications 0`
- Provides non-busy asynchronous background task processing.

---

## 🏁 Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/) (v20 or higher)
- [Docker](https://www.docker.com/) & Docker Compose

### Local Development

1. **Install dependencies:**
   ```bash
   npm install --legacy-peer-deps
   ```

2. **Start Infrastructure Services (PostgreSQL, Kafka, Kafka UI, RabbitMQ, Redis):**
   ```bash
   docker compose -f docker-compose.microservices.yml up -d postgres-db kafka kafka-ui rabbitmq redis
   ```
   - **Kafka UI:** [http://localhost:8080](http://localhost:8080)
   - **RabbitMQ Management:** [http://localhost:15672](http://localhost:15672) (`guest` / `guest`)
   - **PostgreSQL Database:** `localhost:5433` (Database: `order_db`, User: `user`, Pass: `password`)

3. **Run the Development Server:**
   ```bash
   npm run start:dev
   ```
   - HTTP Server running at `http://localhost:3002`.
   - Outbox Worker polling `outbox_events` every 2s.
   - Kafka Consumer listening on `order.status-changed`.
   - RabbitMQ Consumer listening on `orders_queue`.

### Docker Compose Deployment

To build and run the entire topology inside Docker:

```bash
docker compose -f docker-compose.microservices.yml up -d --build
```

---

## 📚 Project Context & Tasks

Detailed specifications, technical architecture summaries, and interview preparation guides:
- [docs/task-1.md](docs/task-1.md) — *Day 10: Event-Driven — Docker Compose Network & Redis Buffer*.
- [docs/task-1-summary.md](docs/task-1-summary.md) — *Summary 1: Redis In-Memory Architecture, Lessons Learned & Scaling*.
- [docs/task-2.md](docs/task-2.md) — *Day 11: Event-Driven — Message Queues with RabbitMQ (AMQP 0-9-1)*.
- [docs/task-2-summary.md](docs/task-2-summary.md) — *Summary 2: AMQP Deep Dive, Manual ACK/NACK, DLQ & Technical Interview Q&A*.
- [docs/task-3.md](docs/task-3.md) — *Day 12: Event-Driven — Apache Kafka, Topics, Partitions & Consumer Groups*.
- [docs/task-3-summary.md](docs/task-3-summary.md) — *Summary 3: Kafka KRaft Deep Dive, Dual Listeners, Partition Key Guarantees & Interview Q&A*.
- [docs/task-4.md](docs/task-4.md) — *Day 13: Event-Driven — Distributed Systems Reliability (Outbox Pattern & Idempotency)*.
- [docs/task-4-summary.md](docs/task-4-summary.md) — *Summary 4: Dual-Write Resolution, SKIP LOCKED Concurrency, Idempotent Consumer & Interview Deep Dive*.
- [docs/postman/notification-service.postman_collection.json](docs/postman/notification-service.postman_collection.json) — *Postman Collection for API & Flow Testing*.
