# День 13: Event-Driven — Надійність розподілених систем (Outbox Pattern & Idempotency)

Сьогодні ми завершуємо третій мікропроєкт — **Event-Driven Microservices Network**. Наша мета — розв'язати найскладнішу та найнебезпечнішу архітектурну проблему розподілених систем: **Dual-Write Problem** (втрату узгодженості між базою даних та брокером повідомлень). Ми спроєктуємо та реалізуємо патерн **Transactional Outbox**, напишемо фоновий воркер з конкурентним блокуванням рядків (`SKIP LOCKED`), розберемо рівні гарантій доставки та реалізуємо ідемпотентний обробник (Idempotent Consumer).

---

## ⏱️ Розклад Дня 13 (6 годин)

| Блок       | Тривалість | Тема                                | Опис                                                                                           |
| :--------- | :--------- | :---------------------------------- | :--------------------------------------------------------------------------------------------- |
| **Блок 1** | 1 год      | Алгоритмічний розігрів (TS/JS)      | Утиліта ідемпотентності (`IdempotencyStore`) на базі детермінованого SHA-256 хешування         |
| **Блок 2** | 2.5 год    | Dual-Write Problem & Outbox Схема   | Анатомія збоїв подвійного запису, створення таблиці `outbox_events` та атомарна SQL-транзакція |
| **Блок 3** | 1.5 год    | Outbox Polling Worker & SKIP LOCKED | Реалізація фонового диспетчера подій, конкурентні воркери, дедуплікація повідомлень            |
| **Блок 4** | 1 год      | Рев'ю та інтерв'ю-підготовка        | Підсумки Проєкту 3: At-least-once, CDC (Debezium WAL) vs Polling, Exactly-once обробка         |

---

## 🛠️ Завдання 1: Проблема подвійного запису (Dual-Write Problem) та схема Outbox

### Мета

Зрозуміти, чому наївна спроба "зберегти в БД і відправити в брокер" у мікросервісах завжди призводить до втрати або неузгодженості даних при високих навантаженнях, та спроєктувати надійну архітектуру **Transactional Outbox**.

### Анатомія проблеми подвійного запису (Dual-Write)

```
Варіант 1 (БД перша):
1. db.orders.save() ----> [Успіх у Postgres]
2. broker.publish() ----> [ЗБІЙ МЕРЕЖІ / Брокер впав]
Наслідок: Замовлення є в системі, але сервіс аналітики та склад нічого про це не знають.

Варіант 2 (Брокер перший):
1. broker.publish() ----> [Успіх у RabbitMQ/Kafka]
2. db.orders.save() ----> [ПОМИЛКА УЗГОДЖЕНОСТІ / OOM / Рестарт Node.js]
Наслідок: Сервіс складу вже списує товари під замовлення, якого взагалі не існує в базі!
```

### Рішення: Transactional Outbox Pattern

Замовлення та спеціальний запис про подію зберігаються в одній реляційній базі даних **в межах єдиної атомарної ACID транзакції**.

### Кроки реалізації

1. **Створення сутності `OutboxEvent` у PostgreSQL:**
   Створи файл `src/orders/entities/outbox-event.entity.ts`:

   ```typescript
   import {
     Entity,
     PrimaryGeneratedColumn,
     Column,
     CreateDateColumn,
     Index,
   } from 'typeorm';

   export enum OutboxStatus {
     PENDING = 'PENDING',
     PROCESSING = 'PROCESSING',
     PUBLISHED = 'PUBLISHED',
     FAILED = 'FAILED',
   }

   @Entity('outbox_events')
   export class OutboxEvent {
     @PrimaryGeneratedColumn('uuid')
     id: string;

     @Column()
     aggregateType: string; // наприклад, 'Order'

     @Column()
     aggregateId: string; // ID замовлення

     @Column()
     eventType: string; // наприклад, 'ORDER_CREATED'

     @Column('jsonb')
     payload: Record<string, any>;

     @Index()
     @Column({
       type: 'enum',
       enum: OutboxStatus,
       default: OutboxStatus.PENDING,
     })
     status: OutboxStatus;

     @Column({ default: 0 })
     retryCount: number;

     @Column({ nullable: true })
     errorMessage: string;

     @CreateDateColumn()
     createdAt: Date;

     @Column({ nullable: true })
     processedAt: Date;
   }
   ```

2. **Атомарне збереження в транзакції `OrdersService`:**

   ```typescript
   @Injectable()
   export class OrdersService {
     constructor(private readonly dataSource: DataSource) {}

     async createOrder(createOrderDto: CreateOrderDto) {
       return this.dataSource.transaction(async (manager) => {
         // 1. Створюємо та зберігаємо саме замовлення
         const order = manager.create(Order, {
           customerEmail: createOrderDto.customerEmail,
           totalPrice: createOrderDto.totalPrice,
           status: 'CREATED',
         });
         const savedOrder = await manager.save(Order, order);

         // 2. В ЦІЙ ЖЕ ТРАНЗАКЦІЇ зберігаємо подію в Outbox!
         const outboxEvent = manager.create(OutboxEvent, {
           aggregateType: 'Order',
           aggregateId: savedOrder.id,
           eventType: 'ORDER_CREATED',
           payload: {
             orderId: savedOrder.id,
             customerEmail: savedOrder.customerEmail,
             totalPrice: savedOrder.totalPrice,
           },
           status: OutboxStatus.PENDING,
         });
         await manager.save(OutboxEvent, outboxEvent);

         // Якщо на цьому етапі вимкнеться живлення — ні замовлення,
         // ні подія не зафіксуються. Якщо зафіксуються — то гарантовано обидва!
         return savedOrder;
       });
     }
   }
   ```

---

## ⚙️ Завдання 2: Outbox Polling Worker з блокуванням `SKIP LOCKED`

### Мета

Реалізувати надійний фоновий воркер, який періодично вичитує події зі статусом `PENDING`, публікує їх у брокер повідомлень (RabbitMQ або Kafka) і позначає як `PUBLISHED`. Забезпечити безпечну паралельну роботу кількох інстансів сервісу за допомогою конструкції `FOR UPDATE SKIP LOCKED`.

### Чому саме `SKIP LOCKED`?

Якщо у нас запущено 3 репліки `order-service`, і кожна з них виконує `SELECT * FROM outbox_events WHERE status = 'PENDING' LIMIT 10`, вони намагатимуться обробити одні й ті самі події. Конструкція PostgreSQL `FOR UPDATE SKIP LOCKED` блокує вибрані рядки для поточної транзакції воркера, а іншим паралельним воркерам наказує **пропускати заблоковані рядки** і брати наступні вільні!

### Кроки реалізації

1. **Створення `OutboxProcessorWorker`:**
   Створи файл `src/orders/workers/outbox-processor.worker.ts`:

   ```typescript
   import { Injectable, Logger } from '@nestjs/common';
   import { Cron, CronExpression } from '@nestjs/schedule';
   import { DataSource } from 'typeorm';
   import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity';
   import { ClientKafka } from '@nestjs/microservices'; // або RabbitMQ Client

   @Injectable()
   export class OutboxProcessorWorker {
     private readonly logger = new Logger(OutboxProcessorWorker.name);
     private isRunning = false;

     constructor(
       private readonly dataSource: DataSource,
       private readonly messageBroker: ClientKafka,
     ) {}

     // Опитування кожні 2 секунди
     @Cron('*/2 * * * * *')
     async processOutboxMessages() {
       if (this.isRunning) return;
       this.isRunning = true;

       try {
         await this.dataSource.transaction(async (manager) => {
           // 1. Конкурентна вибірка з блокуванням SKIP LOCKED
           const pendingEvents = await manager
             .createQueryBuilder(OutboxEvent, 'event')
             .setLock('pessimistic_write')
             .setOnLocked('skip_locked')
             .where('event.status = :status', { status: OutboxStatus.PENDING })
             .orderBy('event.createdAt', 'ASC')
             .take(20)
             .getMany();

           if (pendingEvents.length === 0) return;

           this.logger.log(
             `Виявлено ${pendingEvents.length} подій Outbox для публікації`,
           );

           for (const event of pendingEvents) {
             try {
               // 2. Публікація в брокер
               await this.messageBroker.emit(event.eventType, {
                 key: event.aggregateId,
                 value: event.payload,
               });

               // 3. Оновлення статусу
               event.status = OutboxStatus.PUBLISHED;
               event.processedAt = new Date();
               await manager.save(OutboxEvent, event);

               this.logger.log(
                 `Подія ${event.id} (${event.eventType}) успішно опублікована!`,
               );
             } catch (err: any) {
               this.logger.error(
                 `Помилка відправки події ${event.id}:`,
                 err.message,
               );
               event.retryCount += 1;
               event.errorMessage = err.message;
               if (event.retryCount >= 5) {
                 event.status = OutboxStatus.FAILED;
               }
               await manager.save(OutboxEvent, event);
             }
           }
         });
       } finally {
         this.isRunning = false;
       }
     }
   }
   ```

2. **Тестування надійності:**
   - Зупини брокер повідомлень (`docker stop kafka_broker` або `rabbitmq_broker`).
   - Створи 5 замовлень через `POST /orders`. Переконайся, що замовлення успішно створилися в базі, а в таблиці `outbox_events` з'явилося 5 записів зі статусом `PENDING`.
   - Запусти брокер назад.
   - Спостерігай у логах, як воркер підхоплює всі відкладені події і без жодної втрати доставляє їх у брокер!

---
