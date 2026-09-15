# 📘 Підсумки та Архітектурний Звіт: Завдання 1 та 2 (День 13)

## Надійність розподілених систем: Transactional Outbox Pattern, SKIP LOCKED Concurrency та Idempotent Consumer

---

## 📑 Зміст

1. [Огляд виконаних робіт (Executive Summary)](#1-огляд-виконаних-робіт-executive-summary)
2. [Анатомія проблеми подвійного запису (Dual-Write Problem)](#2-анатомія-проблеми-подвійного-запису-dual-write-problem)
   - [2.1. Чому наївна синхронізація завжди призводить до втрати даних](#21-чому-наївна-синхронізація-завжди-призводить-до-втрати-даних)
   - [2.2. Чому двофазний коміт (2PC) не підходить для мікросервісів](#22-чому-двофазний-коміт-2pc-не-підходить-для-мікросервісів)
3. [Архітектура Transactional Outbox у PostgreSQL](#3-архітектура-transactional-outbox-у-postgresql)
   - [3.1. Схема таблиці `outbox_events` та індексація](#31-схема-таблиці-outbox_events-та-індексація)
   - [3.2. Атомарна транзакція в `OrdersService`](#32-атомарна-транзакція-в-ordersservice)
4. [Outbox Polling Worker та конкурентне блокування `SKIP LOCKED`](#4-outbox-polling-worker-та-конкурентне-блокування-skip-locked)
   - [4.1. Механіка PostgreSQL `FOR UPDATE SKIP LOCKED`](#41-механіка-postgresql-for-update-skip-locked)
   - [4.2. Запобігання гонкам (Race Conditions) та обробка збоїв (Retry Logic)](#42-запобігання-гонкам-race-conditions-та-обробка-збоїв-retry-logic)
5. [Ідемпотентний споживач (Idempotent Consumer) та детерміноване хешування](#5-ідемпотентний-споживач-idempotent-consumer-та-детерміноване-хешування)
   - [5.1. Чому At-least-once доставка вимагає ідемпотентності](#51-чому-at-least-once-доставка-вимагає-ідемпотентності)
   - [5.2. Реалізація `IdempotencyStore` на базі детермінованого SHA-256](#52-реалізація-idempotencystore-на-базі-детермінованого-sha-256)
   - [5.3. Фільтрація дублікатів у `NotificationsController`](#53-фільтрація-дублікатів-у-notificationscontroller)
6. [Блок 4: Відповіді на питання для співбесід (Deep Dive Q&A)](#6-блок-4-відповіді-на-питання-для-співбесід-deep-dive-qa)
   - [Q1: Polling Worker проти Change Data Capture (CDC / Debezium WAL)](#q1-polling-worker-проти-change-data-capture-cdc--debezium-wal)
   - [Q2: Як керувати зростанням таблиці `outbox_events` у високонавантажених системах?](#q2-як-керувати-зростанням-таблиці-outbox_events-у-високонавантажених-системах)
   - [Q3: Що таке семантика Exactly-Once і як вона досягається на практиці?](#q3-що-таке-семантика-exactly-once-і-як-вона-досягається-на-практиці)
   - [Q4: Що станеться, якщо Kafka впаде надовго (накопичення подій і Thundering Herd)?](#q4-що-станеться-якщо-kafka-впаде-надовго-накопичення-подій-і-thundering-herd)
7. [Практичні інструкції з тестування (Postman, pgAdmin та Chaos Test)](#7-практичні-інструкції-з-тестування-postman-pgadmin-та-chaos-test)

---

## 1. Огляд виконаних робіт (Executive Summary)

У рамках 13-го дня було успішно завершено фінальний етап розробки третього навчального мікропроєкту **Event-Driven Microservices Network**. Сервіс `notification-service` розширено промисловими патернами надійності розподілених систем:

1. **Інтеграція реляційної бази даних PostgreSQL 16:**
   - Підключено та налаштовано TypeORM (`@nestjs/typeorm`, `typeorm`, `pg`).
   - У `docker-compose.microservices.yml` сервісу `postgres-db` прокинуто порт `5433:5432` для усунення колізій із локальними інстансами PostgreSQL.
   - Автоматизовано синхронізацію схеми сутностей `orders` та `outbox_events`.

2. **Реалізація патерну Transactional Outbox:**
   - Створено сутність `Order` та сутність журналу подій `OutboxEvent` (з полями `aggregateType`, `aggregateId`, `eventType`, `payload: jsonb`, enum `status` зі статусами `PENDING`, `PROCESSING`, `PUBLISHED`, `FAILED`).
   - На полі `status` сутності `OutboxEvent` створено B-Tree індекс для прискорення вибірки воркером.
   - Метод `OrdersService.createOrder()` реалізовано через єдину атомарну транзакцію `dataSource.transaction`, що унеможливлює розсинхронізацію між створенням замовлення та реєстрацією події.

3. **Фоновий диспетчер подій `OutboxProcessorWorker` із `SKIP LOCKED`:**
   - Підключено планувальник `@nestjs/schedule` з інтервалом опитування 2 секунди (`@Cron('*/2 * * * * *')`).
   - Реалізовано конкурентну безпечну вибірку подій через `setLock('pessimistic_write')` та `setOnLocked('skip_locked')`.
   - Забезпечено захист від накладання тактів опитування через прапорець `isRunning`.
   - Реалізовано механізм повторних спроб (`retryCount`) з фіксацією тексту помилки та переведенням у статус `FAILED` після 5 невдалих спроб.
   - Успішно проведено **Chaos Testing** (зупинка брокера Kafka `docker stop kafka_broker` ➡️ створення замовлень ➡️ старт Kafka ➡️ гарантована доставка накопичених подій без втрат).

4. **Ідемпотентний споживач (Idempotent Consumer) на стороні Kafka:**
   - Створено алгоритмічну утиліту `IdempotencyStore` на базі рекурсивного сортування ключів та генерації детермінованого SHA-256 хешу корисного навантаження.
   - У методі `NotificationsController.handleOrderStatusChanged` реалізовано перевірку відбитка події з TTL у 2 хвилини, що гарантує захист від дублювання обробки при доставці *At-least-once*.

5. **Оновлення Postman-колекції:**
   - Додано запити для створення замовлень через Outbox (`Create Order (Transactional Outbox)`) та валідаційні тести (`Create Order (Validation Error)`).

---

## 2. Анатомія проблеми подвійного запису (Dual-Write Problem)

### 2.1. Чому наївна синхронізація завжди призводить до втрати даних

У мікросервісній архітектурі бізнес-операція часто вимагає **двох дій**:
1. Зберегти стан у власній базі даних (наприклад, стан замовлення в PostgreSQL).
2. Сповістити про це інші мікросервіси через брокер повідомлень (Kafka або RabbitMQ).

```
❌ НАЇВНА СХЕМА 1 (База перша):
[ Клієнт ] ──POST /orders──> [ OrdersService ]
                                   │
                 1. db.save(order) │ (УСПІХ)
                                   ▼
                            [( PostgreSQL )]
                                   │
            2. kafka.emit('order') │ ❌ КАТАСТРОФА: Збій мережі / Брокер впав / OOM
                                   ▼
                            [   Kafka    ]

Наслідок: Замовлення є в БД, гроші списані, але склад і аналітика ніколи не дізнаються про нього!
```

```
❌ НАЇВНА СХЕМА 2 (Брокер перший):
[ Клієнт ] ──POST /orders──> [ OrdersService ]
                                   │
            1. kafka.emit('order') │ (УСПІХ)
                                   ▼
                            [   Kafka    ]
                                   │
                 2. db.save(order) │ ❌ КАТАСТРОФА: Помилка валідації БД / Рестарт Node.js
                                   ▼
                            [( PostgreSQL )]

Наслідок: Повідомлення вже в топіку, склад збирає посилку, а замовлення в системі взагалі не існує!
```

### 2.2. Чому двофазний коміт (2PC) не підходить для мікросервісів

Теоретично проблему можна було б розв'язати розподіленою транзакцією за протоколом **Two-Phase Commit (2PC / XA Transactions)**. Проте в сучасних мікросервісах 2PC практично ніколи не використовується через критичні недоліки:
1. **Блокуюча природа:** координатор транзакції блокує ресурси на всіх вузлах до завершення фази голосування. Якщо один вузол завис — зависає вся система.
2. **Низька пропускна здатність:** високі мережеві затримки роблять 2PC непридатним для систем із тисячами операцій на секунду.
3. **Відсутність підтримки:** більшість сучасних брокерів (зокрема Apache Kafka) та NoSQL баз не підтримують стандартизовані XA-транзакції спільно з реляційними СУБД.

**Єдине надійне архітектурне рішення — Transactional Outbox Pattern.**

---

## 3. Архітектура Transactional Outbox у PostgreSQL

Ідея патерну полягає в тому, щоб **відмовитися від спроби писати у дві системи одночасно**. Замість цього ми пишемо **тільки в одне місце — у PostgreSQL**, де реляційна база гарантує суворі ACID-властивості.

```
✅ СХЕМА TRANSACTIONAL OUTBOX:
[ Клієнт ] ──POST /orders──> [ OrdersService ]
                                   │
                      BEGIN TRANSACTION (ACID)
                      ├─ 1. INSERT INTO orders (...)
                      └─ 2. INSERT INTO outbox_events (..., status='PENDING')
                      COMMIT TRANSACTION
                                   │
                                   ▼
                            [( PostgreSQL )]
                           /                \
                   [Таблиця: orders]    [Таблиця: outbox_events]
                                                  ▲
                                                  │ Polling (SKIP LOCKED)
                                                  │
                                       [OutboxProcessorWorker]
                                                  │
                                                  │ kafka.emit()
                                                  ▼
                                            [   Kafka    ]
```

### 3.1. Схема таблиці `outbox_events` та індексація

Сутність [src/orders/entities/outbox-event.entity.ts](../src/orders/entities/outbox-event.entity.ts):

```typescript
@Entity('outbox_events')
export class OutboxEvent {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  aggregateType: string; // 'Order'

  @Column()
  aggregateId: string; // UUID замовлення

  @Column()
  eventType: string; // 'ORDER_CREATED'

  @Column('jsonb')
  payload: Record<string, any>; // JSON з даними замовлення

  @Index() // B-Tree індекс для прискорення вибірки WHERE status = 'PENDING'
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

> [!IMPORTANT]
> **Чому індекс на полі `status` обов'язковий?**  
> Воркер опитує таблицю кожні 2 секунди: `WHERE status = 'PENDING'`. У міру росту бази в ній накопичуватимуться мільйони записів зі статусом `PUBLISHED`. Без індексу кожен запит воркера виконував би важке послідовне сканування (Sequential Scan). Завдяки індексу запит виконується за частки мілісекунди (Index Scan / Bitmap Index Scan).

### 3.2. Атомарна транзакція в `OrdersService`

У [src/orders/orders.service.ts](../src/orders/orders.service.ts):

```typescript
async createOrder(createOrderDto: CreateOrderDto) {
  return this.dataSource.transaction(async (manager) => {
    // 1. Створюємо і зберігаємо замовлення
    const order = manager.create(Order, {
      customerEmail: createOrderDto.customerEmail,
      totalPrice: createOrderDto.totalPrice,
      status: 'CREATED',
    });
    const savedOrder = await manager.save(Order, order);

    // 2. У тій самій транзакції створюємо подію для Outbox
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

    // Якщо тут зникне живлення — база автоматично зробить ROLLBACK.
    // Якщо транзакція зафіксована — обидва записи гарантовано збережені!
    return savedOrder;
  });
}
```

---

## 4. Outbox Polling Worker та конкурентне блокування `SKIP LOCKED`

### 4.1. Механіка PostgreSQL `FOR UPDATE SKIP LOCKED`

У реальних системах додаток масштабується горизонтально: запущено 3, 5 або 10 паралельних інстансів сервісу. Якщо кожен інстанс запустить воркер опитування:
- **Без блокування:** Усі 3 воркери одночасно оберуть одні й ті самі 20 подій і надішлють у Kafka копії повідомлень.
- **Зі звичайним `FOR UPDATE`:** Перший воркер заблокує рядки, а решта 2 воркери **зависнуть в очікуванні** зняття блокування (Lock Contention).
- **З `FOR UPDATE SKIP LOCKED`:** Перший воркер блокує рядки 1–20. Другий воркер бачить, що рядки 1–20 заблоковані, **пропускає їх** і миттєво бере рядки 21–40!

```
База даних outbox_events (рядки PENDING):
[Row 1] [Row 2] [Row 3] [Row 4] [Row 5] [Row 6] ...
 └─────────────┬─────────────┘   └─────────────┬─────────────┘
               │                               │
        (Заблоковано)                   (Пропущено 1-3,
               │                         взято наступні)
               ▼                               ▼
       [ Worker Instance 1 ]           [ Worker Instance 2 ]
```

### 4.2. Запобігання гонкам (Race Conditions) та обробка збоїв (Retry Logic)

У [src/orders/workers/outbox-processor.worker.ts](../src/orders/workers/outbox-processor.worker.ts):

```typescript
@Cron('*/2 * * * * *')
async processOutboxMessages() {
  if (this.isRunning) return; // Захист від накладання тактів
  this.isRunning = true;

  try {
    await this.dataSource.transaction(async (manager) => {
      const pendingEvents = await manager
        .createQueryBuilder(OutboxEvent, 'event')
        .setLock('pessimistic_write') // FOR UPDATE
        .setOnLocked('skip_locked')   // SKIP LOCKED
        .where('event.status = :status', { status: OutboxStatus.PENDING })
        .orderBy('event.createdAt', 'ASC')
        .take(20)
        .getMany();

      if (pendingEvents.length === 0) return;

      for (const event of pendingEvents) {
        try {
          // Очікуємо підтвердження запису брокером
          await firstValueFrom(
            this.kafkaClient.emit('order.status-changed', {
              key: event.aggregateId, // Partition Key
              value: event.payload,
            }),
          );

          event.status = OutboxStatus.PUBLISHED;
          event.processedAt = new Date();
          await manager.save(OutboxEvent, event);
        } catch (err: any) {
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
```

---

## 5. Ідемпотентний споживач (Idempotent Consumer) та детерміноване хешування

### 5.1. Чому At-least-once доставка вимагає ідемпотентності

Патерн Transactional Outbox забезпечує рівень доставки **At-least-once** (як мінімум один раз).
Чому може виникнути дублікат:
1. Воркер надіслав подію в Kafka брокер (`emit()`).
2. Брокер успішно зберіг повідомлення.
3. Прямо перед рядком `event.status = OutboxStatus.PUBLISHED` сервер раптово перезавантажується.
4. Після старту воркер знову бачить цей запис як `PENDING` і публікує його **вдруге**.

> [!CAUTION]
> Якщо споживач не є ідемпотентним, дублікат події призведе до повторного списання коштів або відправки кількох однакових email-повідомлень клієнту.

### 5.2. Реалізація `IdempotencyStore` на базі детермінованого SHA-256

У [src/common/idempotency.store.ts](../src/common/idempotency.store.ts) реалізовано детерміновану нормалізацію об'єктів (впорядкування ключів) перед хешуванням:

```typescript
import { createHash } from 'node:crypto';

export class IdempotencyStore {
  private readonly store = new Map<string, number>();

  constructor(private readonly defaultTtlMs = 120_000) {}

  public generateHash(payload: unknown): string {
    const sortedString = JSON.stringify(payload, (_, val) =>
      val && typeof val === 'object' && !Array.isArray(val)
        ? Object.keys(val)
            .sort()
            .reduce((acc: Record<string, any>, key) => {
              acc[key] = val[key];
              return acc;
            }, {})
        : val,
    );

    return createHash('sha256').update(sortedString).digest('hex');
  }

  public has(key: string): boolean {
    const expiry = this.store.get(key);
    if (!expiry) return false;
    if (Date.now() > expiry) {
      this.store.delete(key);
      return false;
    }
    return true;
  }

  public set(key: string, ttlMs: number = this.defaultTtlMs): void {
    this.store.set(key, Date.now() + ttlMs);
  }
}
```

### 5.3. Фільтрація дублікатів у `NotificationsController`

У [src/notifications/notifications.controller.ts](../src/notifications/notifications.controller.ts):

```typescript
@MessagePattern('order.status-changed')
async handleOrderStatusChanged(
  @Payload() message: any,
  @Ctx() context: KafkaContext,
) {
  const rawMsg = context.getMessage();
  const partition = context.getPartition();
  const offset = rawMsg.offset;
  const key = rawMsg.key?.toString();

  // 1. Детермінований відбиток
  const eventFingerprint = this.idempotencyStore.generateHash({
    key,
    payload: message,
  });

  // 2. Відсікання дублікатів
  if (this.idempotencyStore.has(eventFingerprint)) {
    console.warn(
      `[Kafka Consumer] ⚠️ [DUPLICATE SKIPPED] Event for order ${key} has already been processed!`,
    );
    return;
  }

  // 3. Фіксація ключа
  this.idempotencyStore.set(eventFingerprint);

  console.log(
    `[Kafka Consumer] 📥 [FIRST PROCESSING] Partition: ${partition} | Offset: ${offset} | Key: ${key}`,
  );

  // 4. Безпечне виконання бізнес-дії
  await this.notificationsService.create({
    orderId: key || message?.orderId,
    customerEmail: message?.customerEmail || 'customer@example.com',
    message: `Order #${key} successfully processed!`,
  });
}
```

---

## 6. Блок 4: Відповіді на питання для співбесід (Deep Dive Q&A)

### Q1: Polling Worker проти Change Data Capture (CDC / Debezium WAL)

| Критерій | Polling Publisher (`SKIP LOCKED`) | Change Data Capture (CDC / Debezium WAL) |
| :--- | :--- | :--- |
| **Принцип роботи** | Періодичний SQL-запит до таблиці (`SELECT ... FOR UPDATE SKIP LOCKED`) | Читання бінарного журналу транзакцій БД (Postgres WAL / MySQL binlog) |
| **Навантаження на БД** | Генерує періодичні SQL-запити та створює операції блокування/оновлення | **Мінімальне**: читає журнал послідовно з диска, не виконуючи SQL |
| **Затримка (Latency)** | Залежить від інтервалу опитування (наприклад, 1–2 секунди) | **Субмілісекундна (Real-time)**: подія публікується миттєво після фіксації в WAL |
| **Складність інфраструктури** | **Мінімальна**: звичайний крон у коді застосунку, жодних додаткових сервісів | **Висока**: вимагає запуску Kafka Connect, Debezium, налаштування слотів реплікації |
| **Коли обирати?** | Малі та середні проєкти, MVP, простота підтримки та розгортання | High-load системи, фінансові платформи, мільйони подій на добу |

---

### Q2: Як керувати зростанням таблиці `outbox_events` у високонавантажених системах?

Якщо в системі створюються мільйони замовлень, таблиця `outbox_events` буде швидко розростатися, споживаючи дисковий простір та сповільнюючи індекси.
**Виробничі стратегії очищення (Purging & Archiving):**
1. **Фоновий очисник (Housekeeping Job):**
   Окремий нічний CRON або фоновий процес, що видаляє успішно відправлені події старше певного вікна:
   ```sql
   DELETE FROM outbox_events WHERE status = 'PUBLISHED' AND "processedAt" < NOW() - INTERVAL '7 days';
   ```
2. **PostgreSQL Declarative Partitioning:**
   Секціонування таблиці за датою (`PARTITION BY RANGE (createdAt)`). Наприкінці місяця застаріла секція видаляється миттєвою командою `DROP TABLE outbox_events_2026_08`, що відбувається за $O(1)$ без навантаження на диск та блокувань транзакцій.

---

### Q3: Що таке семантика Exactly-Once і як вона досягається на практиці?

> **«Exactly-once delivery in distributed networks is physically impossible.»**

Через ненадійність мережі (падіння пакетів, таймаути) брокер не може гарантувати, що повідомлення дійде рівно один раз. Можлива лише семантика **At-least-once**.
Проте **Exactly-Once Processing** (кінцевий ефект рівно одноразової обробки) досягається за формулою:
$$\text{At-Least-Once Delivery} + \text{Idempotent Consumer} = \text{Effectively Exactly-Once}$$

**Методи забезпечення ідемпотентності на стороні споживача:**
1. **Унікальний бізнес-ключ (Idempotency Key):** створення таблиці `processed_events (event_id PRIMARY KEY)` у базі споживача. Спроба вставити дублікат завершується `ON CONFLICT DO NOTHING`.
2. **Умовний запис (Optimistic Locking / State Machine):** переведення замовлення зі статусу `CREATED` у `PAID` можливе лише якщо поточний статус дійсно `CREATED`.
3. **Криптографічний хеш із TTL (In-Memory / Redis):** як реалізовано в нашому `IdempotencyStore`.

---

### Q4: Що станеться, якщо Kafka впаде надовго (накопичення подій і Thundering Herd)?

Якщо брокер був недоступний 2 години, у таблиці `outbox_events` може накопичитися 100 000 подій.
Коли брокер відновиться, наївний воркер може спробувати відправити всі 100 000 подій одночасно:
- Це призведе до вичерпання пам'яті Node.js (Out-Of-Memory).
- Перевантажить брокер повідомлень (Thundering Herd Problem).

**Як наш воркер захищений від цього:**
1. **Batching (`take(20)`):** воркер обмежує кожну вибірку фіксованим лімітом (20 записів).
2. **Пагінація за часом (`orderBy('event.createdAt', 'ASC')`):** старіші події відправляються першими, зберігаючи хронологію.
3. **Прапорець `isRunning`:** наступний такт CRON не почнеться, поки не завершиться попередня пачка.

---

## 7. Практичні інструкції з тестування (Postman, pgAdmin та Chaos Test)

### Крок 1. Перевірка створення замовлення через Postman
1. Відкрийте Postman ➡️ Колекція `Notification Service API`.
2. Виконайте запит **`Create Order (Transactional Outbox)`**:
   - **Method:** `POST`
   - **URL:** `http://localhost:3002/orders`
   - **Body:**
     ```json
     {
       "customerEmail": "student@example.com",
       "totalPrice": 1250.00
     }
     ```
3. Відповідь сервера: `201 Created` з полями `id` (UUID), `status: "CREATED"`.

---

### Крок 2. Перевірка таблиць через pgAdmin / psql
Виконайте запит у Query Tool:
```sql
SELECT id, "customerEmail", "totalPrice", status, "createdAt" FROM orders;
SELECT id, "aggregateId", "eventType", status, "retryCount", "processedAt" FROM outbox_events;
```
**Результат:**
- `orders.id` повністю збігається з `outbox_events.aggregateId`.
- Воркер автоматично змінив статус події з `PENDING` на `PUBLISHED`.
- Поле `processedAt` містить час успішної доставки.

---

### Крок 3. Chaos Testing (Стрес-тест відмови брокера)
1. Зупиніть брокер:
   ```bash
   docker stop kafka_broker
   ```
2. Відправте 3 нових замовлення через Postman. Замовлення створюються успішно (HTTP 201).
3. Подивіться в логи: воркер кожні 2 секунди фіксує помилку з'єднання з Kafka та інкрементує `retryCount` у базі. Події залишаються в `PENDING`.
4. Запустіть брокер:
   ```bash
   docker start kafka_broker
   ```
5. Спостерігайте в консолі: воркер миттєво підхоплює всі 3 події, доставляє їх у топік Kafka, споживач їх обробляє, а статуси в базі стають `PUBLISHED`. **Втрата даних = 0%!**
