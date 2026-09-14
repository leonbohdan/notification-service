# День 12: Event-Driven — Apache Kafka: Стрімінг подій, топіки та партиції

Сьогодні ми переходимо від черг повідомлень до розподіленої платформи стрімінгу подій **Apache Kafka**. Ми розберемося з архітектурою розподіленого незмінного журналу фіксації (Append-Only Commit Log), піднімемо кластер Kafka у сучасному режимі **KRaft** (без ZooKeeper) разом із вебінтерфейсом Kafka UI, реалізуємо публікацію та читання подій у NestJS через `kafkajs`, гарантуємо збереження порядку подій за допомогою Partition Keys та дослідимо поведінку Consumer Groups.

---

## ⏱️ Розклад Дня 12 (6 годин)

| Блок       | Тривалість | Тема                                | Опис                                                                                          |
| :--------- | :--------- | :---------------------------------- | :-------------------------------------------------------------------------------------------- |
| **Блок 1** | 1 год      | Алгоритмічний розігрів (TS/JS)      | Кільцевий буфер (`CircularBuffer`) фіксованого розміру у пам'яті за $O(1)$ без зсуву масиву    |
| **Блок 2** | 2.5 год    | Kafka у Docker (KRaft) та NestJS    | Розгортання кластера без ZooKeeper, Kafka UI, налаштування NestJS Kafka Client (`kafkajs`)    |
| **Блок 3** | 1.5 год    | Партиції, ключі та Consumer Groups  | Гарантія порядку за Partition Key, партиціонування топіка, балансування споживачів            |
| **Блок 4** | 1 год      | Рев'ю та інтерв'ю-підготовка        | Kafka vs RabbitMQ: Pull vs Push, Retention Log, Rebalance Storm, семантика доставки           |

---

## 🛠️ Завдання 1: Розгортання кластера Apache Kafka (KRaft) та Kafka UI

### Мета

Розгорнути сучасний кластер Apache Kafka без ZooKeeper (режим KRaft — Kafka Raft Metadata mode), налаштувати вебінтерфейс керування Kafka UI та підключити мікросервісний клієнт у NestJS.

### Кроки реалізації

1. **Додавання Kafka (KRaft) до `docker-compose.microservices.yml`:**

   ```yaml
   kafka:
     image: apache/kafka:3.7.0
     container_name: kafka_broker
     ports:
       - "9092:9092"
     environment:
       # KRaft налаштування: вузол виступає і брокером, і контролером
       KAFKA_NODE_ID: 1
       KAFKA_PROCESS_ROLES: broker,controller
       KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
       KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
       KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
       KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
       KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
       KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
       KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
       KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
       KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
       KAFKA_NUM_PARTITIONS: 3
     networks:
       - microservices_net

   kafka-ui:
     image: provectuslabs/kafka-ui:latest
     container_name: kafka_ui
     ports:
       - "8080:8080"
     environment:
       KAFKA_CLUSTERS_0_NAME: local-cluster
       KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
     depends_on:
       - kafka
     networks:
       - microservices_net
   ```

2. **Встановлення клієнта Kafkajs у NestJS:**
   ```bash
   npm install @nestjs/microservices kafkajs
   ```

3. **Конфігурація Kafka Client у `order-service` (Продюсер):**
   У `orders.module.ts`:

   ```typescript
   import { ClientsModule, Transport } from '@nestjs/microservices';

   @Module({
     imports: [
       ClientsModule.register([
         {
           name: 'KAFKA_PRODUCER_SERVICE',
           transport: Transport.KAFKA,
           options: {
             client: {
               clientId: 'order-service-producer',
               brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
             },
             producer: {
               allowAutoTopicCreation: true,
             },
           },
         },
       ]),
     ],
   })
   export class OrdersModule {}
   ```

---

## 🎯 Завдання 2: Гарантія порядку за Partition Key та Consumer Groups

### Мета

Налаштувати топік із 3 партиціями, навчитися керувати розподілом подій за партиціями за допомогою Partition Key (збереження порядку подій конкретного замовлення) та реалізувати масштабовану групу споживачів (Consumer Group) в `analytics-service`.

### Кроки реалізації

1. **Публікація подій із ключем партиціонування (Partition Key):**
   У `order.service.ts`:
   Коли статус замовлення змінюється (`CREATED` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`), події мають оброблятися **суворо в цьому хронологічному порядку**.

   ```typescript
   import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
   import { ClientKafka } from '@nestjs/microservices';

   @Injectable()
   export class OrdersService implements OnModuleInit {
     constructor(
       @Inject('KAFKA_PRODUCER_SERVICE')
       private readonly kafkaClient: ClientKafka,
     ) {}

     async onModuleInit() {
       // Підключення продюсера до брокера при старті
       await this.kafkaClient.connect();
     }

     async publishOrderStatusEvent(orderId: string, status: string, payload: any) {
       // КРИТИЧНО: передаємо orderId як "key"!
       // Завдяки хешуванню ключа (murmur2) всі події з однаковим orderId
       // гарантовано потраплять в одну й ту саму партицію!
       return this.kafkaClient.emit('order.status-changed', {
         key: orderId,
         value: {
           orderId,
           status,
           payload,
           timestamp: new Date().toISOString(),
         },
       });
     }
   }
   ```

2. **Підключення Споживача (`analytics-service`) з Consumer Group:**
   У `main.ts` сервісу аналітики:

   ```typescript
   app.connectMicroservice<MicroserviceOptions>({
     transport: Transport.KAFKA,
     options: {
       client: {
         brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
       },
       consumer: {
         groupId: 'analytics-consumers-group', // Спільна група
         allowAutoTopicCreation: true,
       },
     },
   });
   ```

3. **Контролер підписки на топік у `analytics-service`:**

   ```typescript
   import { Controller } from '@nestjs/common';
   import { MessagePattern, Payload, Ctx, KafkaContext } from '@nestjs/microservices';

   @Controller()
   export class AnalyticsEventsController {
     @MessagePattern('order.status-changed')
     async handleOrderStatusChange(@Payload() message: any, @Ctx() context: KafkaContext) {
       const rawMsg = context.getMessage();
       const partition = context.getPartition();
       const offset = rawMsg.offset;

       console.log(
         `[Analytics Consumer] Партиція: ${partition}, Offset: ${offset}, Ключ: ${rawMsg.key?.toString()}, Статус: ${message.status}`,
       );

       // Оновлення або агрегація аналітики в MongoDB
     }
   }
   ```

4. **Дослідження поведінки в Kafka UI:**
   - Відкрий `http://localhost:8080`.
   - Знайди топік `order.status-changed`. Перевір розподіл повідомлень по трьох партиціях.
   - Відправ кілька подій з однаковим ключем `order-123` та різними статусами: переконайся, що всі вони потрапили в одну партицію і їхні offset ідуть строго послідовно (0, 1, 2...).
