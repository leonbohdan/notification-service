# 📘 Підсумки та Архітектурний Звіт: Завдання 1, 2 та 3 (День 12)

## Event-Driven Architecture, Apache Kafka (KRaft), Partition Keys, Consumer Groups та Dual Listeners

---

## 📑 Зміст

1. [Огляд виконаних робіт (Executive Summary)](#1-огляд-виконаних-робіт-executive-summary)
2. [Архітектурні відмінності: Apache Kafka vs RabbitMQ](#2-архітектурні-відмінності-apache-kafka-vs-rabbitmq)
   - [2.1. Концептуальна різниця: Commit Log проти Message Broker](#21-концептуальна-різниця-commit-log-проти-message-broker)
   - [2.2. Порівняльна таблиця: Kafka vs RabbitMQ](#22-порівняльна-таблиця-kafka-vs-rabbitmq)
3. [Розгортання Kafka у режимі KRaft (без ZooKeeper)](#3-розгортання-kafka-у-режимі-kraft-без-zookeeper)
   - [3.1. Що таке KRaft (Kafka Raft Metadata Mode)?](#31-що-таке-kraft-kafka-raft-metadata-mode)
   - [3.2. Класична пастка Docker та розв'язання через Dual Listeners](#32-класична-пастка-docker-та-розвязання-через-dual-listeners)
4. [Гарантія порядку подій через Partition Key](#4-гарантія-порядку-подій-через-partition-key)
   - [4.1. Алгоритм хешування Murmur2 та природа колізій](#41-алгоритм-хешування-murmur2-та-природа-колізій)
   - [4.2. Чому одне замовлення завжди обробляється послідовно?](#42-чому-одне-замовлення-завжди-обробляється-послідовно)
5. [Реалізація в NestJS: Гібридний додаток (HTTP + RabbitMQ + Kafka)](#5-реалізація-в-nestjs-гібридний-додаток-http--rabbitmq--kafka)
   - [5.1. Конфігурація споживача у `main.ts`](#51-конфігурація-споживача-у-maints)
   - [5.2. Програмний продюсер `ClientKafka`: `emit()` проти `send()`](#52-програмний-продюсер-clientkafka-emit-проти-send)
   - [5.3. Обробник подій у контролері (`KafkaContext`, `Partition`, `Offset`, `Key`)](#53-обробник-подій-у-контролері-kafkacontext-partition-offset-key)
6. [Алгоритмічний блок: Кільцевий буфер (`CircularBuffer`) за $O(1)$](#6-алгоритмічний-блок-кільцевий-буфер-circularbuffer-за-o1)
7. [Блок 4: Відповіді на питання (Deep Dive Q&A)](#7-блок-4-відповіді-на-питання-deep-dive-qa)
   - [Q1: Pull vs Push — чому Kafka обрала опитування (polling)?](#q1-pull-vs-push--чому-kafka-обрала-опитування-polling)
   - [Q2: Що таке Rebalance Storm і як його мінімізувати?](#q2-що-таке-rebalance-storm-і-як-його-мінімізувати)
   - [Q3: Семантика доставки: At-most-once, At-least-once, Exactly-once](#q3-семантика-доставки-at-most-once-at-least-once-exactly-once)
   - [Q4: Що станеться, якщо споживачів у Consumer Group більше, ніж партицій?](#q4-що-станеться-якщо-споживачів-у-consumer-group-більше-ніж-партицій)
8. [Практичні інструкції з тестування (Postman & Kafka UI)](#8-практичні-інструкції-з-тестування-postman--kafka-ui)

---

## 1. Огляд виконаних робіт (Executive Summary)

У рамках 12-го дня розробки сервіс `notification-service` було розширено підтримкою розподіленої платформи стрімінгу подій **Apache Kafka 3.7.0**:

1. **Сучасний кластер Kafka у режимі KRaft (без ZooKeeper):**
   - Розгорнуто брокер у режимі суміщеного контролера та брокера (`broker,controller`) без застарілої залежності від Apache ZooKeeper.
   - Додано вебінтерфейс **Kafka UI** (`provectuslabs/kafka-ui:latest`) на порту `8080` для наочного аудиту топіків, партицій, зміщень (offsets) та споживачів.
   - Сконфігуровано **Dual Listeners** (`INTERNAL://kafka:29092` для внутрішньої мережі Docker та `EXTERNAL://localhost:9092` для хост-машини), що вирішило критичну проблему недоступності брокера з локального процесу розробки.

2. **Підтримка мікросервісного транспорту Kafka у NestJS:**
   - Встановлено рушій `kafkajs` (`package.json`).
   - `src/main.ts` трансформовано у трирівневий гібридний додаток: **HTTP REST (порт 3002) + RabbitMQ (черга AMQP) + Kafka (Consumer Group `notification-kafka-group`)**.

3. **Створення топіка та забезпечення порядку за Partition Key:**
   - Створено топік `order.status-changed` із **3 партиціями**.
   - Реалізовано програмний продюсер через `ClientsModule` та `ClientKafka` (`src/notifications/notifications.service.ts`), який передає `orderId` у якості ключа повідомлення (`key`).
   - Доведено на практиці, що завдяки хешуванню ключа всі події одного замовлення (`CREATED` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`) потрапляють в одну й ту саму партицію і зберігають строгий порядок слідування.

4. **Інструменти тестування:**
   - Оновлено колекцію Postman (`docs/postman/notification-service.postman_collection.json`) запитом `Kafka Test Order Flow` для швидкого моделювання життєвого циклу замовлень через HTTP-запит.

---

## 2. Архітектурні відмінності: Apache Kafka vs RabbitMQ

### 2.1. Концептуальна різниця: Commit Log проти Message Broker

Головна помилка інженерів-початківців — вважати Kafka «просто ще однією чергою повідомлень»:

```
[ RabbitMQ: Традиційна черга повідомлень ]
Продюсер ──> [ Exchange ] ──> [ Queue ] ──(Push)──> Споживач
                                  │
                          (ACK: повідомлення
                           видаляється з черги)

[ Apache Kafka: Незмінний розподілений журнал фіксації (Commit Log) ]
Продюсер ──(Key)──> [ Топік ] ──> [ Партиція 0: [0][1][2][3][4]... ]
                                  [ Партиція 1: [0][1][2][3]... ]
                                  [ Партиція 2: [0][1][2][3][4][5]... ]
                                                    ▲
                                            (Споживач сам читає
                                             та зсуває свій Offset.
                                             Дані зберігаються на диску!)
```

1. **RabbitMQ — Smart Broker, Dumb Consumer:**
   - Брокер бере на себе всю складну логіку маршрутизації (Direct, Fanout, Topic, Headers).
   - Повідомлення зберігається в оперативній пам'яті (або на диску для `durable`), доки його не отримає споживач. Після підтвердження (`ACK`) повідомлення **видаляється**.
   - Брокер сам активно виштовхує (`Push`) повідомлення підключеному клієнту.

2. **Apache Kafka — Dumb Broker, Smart Consumer:**
   - Брокер — це надшвидкий, послідовний журнал фіксації (Append-Only Log на диску), який використовує кеш сторінок ОС (Page Cache) та системний виклик `sendfile` (Zero-Copy).
   - Повідомлення **НІКОЛИ не видаляються** при читанні. Вони зберігаються за політикою утримання часу або обсягу (`log.retention.hours=168` — наприклад, 7 діб).
   - Споживач сам періодично запитує (`Pull`) пачки даних у брокера і фіксує свій числовий покажчик — **Offset** (зміщення).

---

### 2.2. Порівняльна таблиця: Kafka vs RabbitMQ

| Критерій                  | RabbitMQ (AMQP 0-9-1)                                    | Apache Kafka                                                               |
| :------------------------ | :------------------------------------------------------- | :------------------------------------------------------------------------- |
| **Архітектурна модель**   | Черга повідомлень (Queue-based broker)                   | Розподілений журнал подій (Log-centric stream)                             |
| **Модель передачі даних** | **Push:** брокер проштовхує події споживачам             | **Pull:** споживач сам забирає (poll) дані                                 |
| **Життя повідомлення**    | Видаляється відразу після успішного `ACK`                | Зберігається визначений час (`retention period`), незалежно від споживачів |
| **Перечитування історії** | Неможливе (повідомлень уже немає)                        | Можливе в будь-який момент (скидання `offset` назад)                       |
| **Гарантія порядку**      | Тільки в межах однієї черги за наявності 1 споживача     | **Сувора гарантія в межах кожної окремої партиції**                        |
| **Пропускна здатність**   | Десятки тисяч повідомлень на секунду                     | Мільйони повідомлень на секунду (Zero-copy, Batching)                      |
| **Масштабування читання** | Багато споживачів конкурують за 1 чергу (Round-robin)    | Кількість споживачів у групі обмежена кількістю партицій                   |
| **Ідеальний сценарій**    | Комплексна бізнес-маршрутизація, RPC, фонові задачі, DLQ | Стрімінг подій, телеметрія, аудиторські логи, CQRS, CDC                    |

---

## 3. Розгортання Kafka у режимі KRaft (без ZooKeeper)

### 3.1. Що таке KRaft (Kafka Raft Metadata Mode)?

Історично кластери Kafka спиралися на **Apache ZooKeeper** для збереження метаданих, вибору лідера та конфігурації кластера. Це створювало низку проблем:

- Необхідність адмініструвати, масштабувати та синхронізувати дві окремі розподілені системи.
- Обмеження на максимальну кількість партицій (через накладні витрати синхронізації із ZooKeeper).
- Повільне відновлення лідера брокерів при аваріях.

Починаючи з версії Kafka 3.x, стандартом став протокол **KRaft** (KIP-500). Метадані кластера тепер зберігаються у спеціальному внутрішньому топіку `@metadata` самої Kafka, а вибір лідерів регулюється алгоритмом консенсусу **Raft**.

---

### 3.2. Класична пастка Docker та розв'язання через Dual Listeners

Під час налаштування `docker-compose.microservices.yml` виникла типова помилка взаємодії Kafka з контейнерами:

```text
Connection error: getaddrinfo EAI_AGAIN kafka (або ENOTFOUND kafka)
```

#### Анатомія проблеми

1. Клієнт на хост-машині підключається до `localhost:9092`.
2. Kafka приймає TCP-з'єднання і повертає свої метадані: _«Моя публічна адреса для надсилання даних: `kafka:9092`»_.
3. Клієнт на вашому комп'ютері намагається розпізнати хост `kafka`, але локальний DNS нічого про нього не знає (ім'я `kafka` існує лише всередині мережі Docker `microservices_net`).

#### Архітектурне вирішення (Dual Listeners)

Ми налаштували брокер із двома незалежними точками входу:

1. **INTERNAL (`29092`):** для контейнерів у Docker-мережі (`kafka-ui`, інші мікросервіси).
2. **EXTERNAL (`9092`):** для локальних клієнтів із хост-машини (NestJS у режимі `npm run start:dev`, Postman, локальні тести).
3. **`KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL`:** явна вказівка брокеру використовувати саме внутрішній лісенер для службової комунікації.

Конфігурація у `docker-compose.microservices.yml`:

```yaml
kafka:
  image: apache/kafka:3.7.0
  container_name: kafka_broker
  ports:
    - '9092:9092' # Зовнішній доступ із хоста
    - '29092:29092' # Внутрішній порт мережі
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
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
    - '8080:8080'
  environment:
    KAFKA_CLUSTERS_0_NAME: local-cluster
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
  depends_on:
    - kafka
  networks:
    - microservices_net
```

---

## 4. Гарантія порядку подій через Partition Key

### 4.1. Алгоритм хешування Murmur2 та природа колізій

У Kafka топік ділиться на **партиції** (у нашому випадку — 3). Партиція є базовою одиницею паралелізму.

Коли продюсер надсилає повідомлення, визначається, у яку партицію воно потрапить:

- **Якщо ключ відсутній (`key = null`):** повідомлення розподіляються за круговим алгоритмом (Sticky Partitioner / Round-robin).
- **Якщо ключ вказано (`key = 'order-101'`):** номер партиції обчислюється строго детерміновано:
  $$\text{Partition} = |\text{murmur2}(\text{key})| \pmod{\text{кількість партицій}}$$

#### Чому `order-777` та `order-999` опинилися в одній партиції?

Під час тестування обидва ключі потрапили у `Партицію 0`. Це **колізія залишку від ділення**:

- При 3 партиціях існує лише 3 можливих результати: `0`, `1` або `2`.
- Будь-який ключ має ймовірність $\frac{1}{3} \approx 33.3\%$ потрапити в партицію `0`.
- Коли ми відправили `order-101`, залишок від ділення склав `1`, і замовлення пішло в `Партицію 1`.

---

### 4.2. Чому одне замовлення завжди обробляється послідовно?

У розподілених системах одна з найбільших небезпек — **Race Condition** при зміні статусів (наприклад, коли подія `SHIPPED` обганяє подію `PAID`).

У Kafka порядок гарантується **виключно в межах однієї партиції**:

1. Подія 1: `{ key: 'order-101', status: 'CREATED' }` $\rightarrow$ `murmur2('order-101') % 3 = 1` $\rightarrow$ **Partition 1, Offset 0**
2. Подія 2: `{ key: 'order-101', status: 'PAID' }` $\rightarrow$ `murmur2('order-101') % 3 = 1` $\rightarrow$ **Partition 1, Offset 1**
3. Подія 3: `{ key: 'order-101', status: 'SHIPPED' }` $\rightarrow$ `murmur2('order-101') % 3 = 1` $\rightarrow$ **Partition 1, Offset 2**

Оскільки споживач вичитує кожну окрему партицію строго послідовно, зміщення зростають як $0 \rightarrow 1 \rightarrow 2$, що **фізично виключає порушення хронологічного порядку**!

---

## 5. Реалізація в NestJS: Гібридний додаток (HTTP + RabbitMQ + Kafka)

### 5.1. Конфігурація споживача у `main.ts`

Сервіс `notification-service` є трифункціональним. У файлі `src/main.ts`:

```typescript
// 1. Слухач черги RabbitMQ (AMQP)
app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.RMQ,
  options: {
    urls: [process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672'],
    queue: 'orders_queue',
    noAck: false,
    queueOptions: {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'orders.dlx',
        'x-dead-letter-routing-key': 'orders.dead_letter',
      },
    },
  },
});

// 2. Слухач топіків Apache Kafka
app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.KAFKA,
  options: {
    client: {
      brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    },
    consumer: {
      groupId: 'notification-kafka-group',
      allowAutoTopicCreation: true,
    },
  },
});

// Старт усіх мікросервісів та HTTP сервера
await app.startAllMicroservices();
await app.listen(port);
```

---

### 5.2. Програмний продюсер `ClientKafka`: `emit()` проти `send()`

У `src/notifications/notifications.module.ts` зареєстровано продюсер:

```typescript
ClientsModule.register([
  {
    name: 'KAFKA_PRODUCER_SERVICE',
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'notification-service-producer',
        brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
      },
      producer: {
        allowAutoTopicCreation: true,
      },
    },
  },
]);
```

У `src/notifications/notifications.service.ts`:

```typescript
@Injectable()
export class NotificationsService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_PRODUCER_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    // Явне з'єднання обов'язкове для Kafka перед першою публікацією
    await this.kafkaClient.connect();
  }

  async publishOrderStatusEvent(
    orderId: string,
    status: string,
    payload: any = {},
  ) {
    // ВАЖЛИВО: key передається як окреме поле поряд із value
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

> [!TIP]
> **Pro-Tip (RxJS & `lastValueFrom`):**
> Метод `this.kafkaClient.emit()` повертає `Observable`. Якщо вам потрібно дочекатися підтвердження запису брокером перед переходом до наступного рядка, використовуйте `await lastValueFrom(this.kafkaClient.emit(...))` з бібліотеки `'rxjs'`.

---

### 5.3. Обробник подій у контролері (`KafkaContext`, `Partition`, `Offset`, `Key`)

У `src/notifications/notifications.controller.ts`:

```typescript
@MessagePattern('order.status-changed')
async handleOrderStatusChanged(
  @Payload() message: any,
  @Ctx() context: KafkaContext,
) {
  const rawMsg = context.getMessage();
  const partition = context.getPartition();
  const offset = rawMsg.offset;
  const key = rawMsg.key?.toString(); // Ключ зчитується з буфера через toString()

  console.log(
    `[Kafka Consumer] 📥 Received event:`,
    `Partition: ${partition} | Offset: ${offset} | Key: ${key} | Status: ${message?.status}`,
  );
}
```

---

## 6. Алгоритмічний блок: Кільцевий буфер (`CircularBuffer`) за $O(1)$

У файлі `task-service/index.day_12.ts` реалізовано кільцевий буфер фіксованого розміру для ефективного збереження останніх подій у пам'яті:

```typescript
export class CircularBuffer<T> {
  private buffer: (T | undefined)[];
  private head = 0; // Звідки читаємо (найстаріший елемент)
  private tail = 0; // Куди пишемо (наступна позиція)
  private count = 0; // Поточна кількість збережених елементів
  private readonly capacity: number;

  constructor(capacity: number) {
    if (capacity <= 0) throw new Error('Місткість має бути більше 0');
    this.capacity = capacity;
    this.buffer = new Array(capacity);
  }

  public push(item: T): boolean {
    this.buffer[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;

    if (this.isFull()) {
      // Якщо буфер заповнений, зсуваємо покажчик найстарішого елемента (витіснення)
      this.head = (this.head + 1) % this.capacity;
    } else {
      this.count++;
    }
    return true;
  }

  public pop(): T | undefined {
    if (this.isEmpty()) return undefined;
    const item = this.buffer[this.head];
    this.buffer[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.count--;
    return item;
  }

  public isFull(): boolean {
    return this.count === this.capacity;
  }

  public isEmpty(): boolean {
    return this.count === 0;
  }
}
```

**Чому це важливо на інтерв'ю:**
Використання звичайного масиву `Array.push()` та `Array.shift()` для черги призводить до складності $O(N)$ через зсув усіх елементів масиву в пам'яті при кожному `shift()`. Кільцевий буфер із залишковою арифметикою `(index + 1) % capacity` гарантує виконання операцій за суворе $O(1)$ без виділення додаткової пам'яті.

---

## 7. Блок 4: Відповіді на питання (Deep Dive Q&A)

### Q1: Pull vs Push — чому Kafka обрала опитування (polling)?

- **Проблема Push-моделі (RabbitMQ):**
  Якщо продюсер генерує 10 000 подій/сек, а споживач може обробити лише 1 000 подій/сек, брокер затопить споживача повідомленнями, вичерпавши його оперативну пам'ять (потрібне жорстке регулювання через `prefetch_count`).
- **Перевага Pull-моделі (Kafka):**
  Споживач запитує дані сам: `poll(maxRecords: 500)`. Якщо споживач виконує складну операцію, він просто не надсилає наступний `poll()`, поки не закінчить поточну партію. Це забезпечує **ідеальний захист від перевантаження (Natural Backpressure)**.

---

### Q2: Що таке Rebalance Storm і як його мінімізувати?

- **Суть проблеми:** Коли в Consumer Group додається або відпадає інстанс споживача (наприклад, через довгу паузу GC або перезапуск пода в Kubernetes), координатор кластера ініціює **Rebalance** — перерозподіл партицій.
- **Eager Rebalance (Старий підхід):** Усі споживачі відмовляються від усіх своїх партицій, перестають читати події (_Stop-The-World_), чекають нового плану і починають заново. Це спричиняє затримки та пікові навантаження (_Storm_).
- **Cooperative Sticky Assignor (Сучасний підхід):** Перерозподіляються лише ті партиції, які реально мігрують. Споживачі, чиї партиції не змінюються, продовжують безперервно обробляти події без паузи.

---

### Q3: Семантика доставки: At-most-once, At-least-once, Exactly-once

1. **At-most-once (Максимум один раз):**
   - Offset комітиться **до** обробки повідомлення.
   - _Ризик:_ якщо сервіс впаде під час обробки, повідомлення буде назавжди втрачено.
2. **At-least-once (Щонайменше один раз — галузевий стандарт):**
   - Offset комітиться **після** успішної обробки.
   - _Ризик:_ якщо сервіс впаде після обробки, але до коміту offset, при перезапуску повідомлення прочитається знову.
   - _Вимога:_ споживач обов'язково повинен бути **ідемпотентним** (перевірка `processed_orders` у базі даних).
3. **Exactly-once Semantics (EOS / Рівно один раз):**
   - Досягається зв'язкою: ідемпотентний продюсер (`enable.idempotence=true`) + транзакційний API Kafka + комбінований запис стану та зміщень в одну транзакцію.

---

### Q4: Що станеться, якщо споживачів у Consumer Group більше, ніж партицій?

- **Правило 1:** Одну партицію в межах однієї Consumer Group може одночасно читати **лише один споживач**.
- **Відповідь:** Якщо у топіку **3 партиції**, а в Consumer Group запущено **4 сервіси**, то 3 споживачі отримають по 1 партиції, а **4-й споживач буде повністю простоювати (idle)**, виступаючи лише як гарячий резерв на випадок падіння одного з трьох.
- **Висновок:** Кількість партицій топіка є верхньою межею паралелізму для Consumer Group.

---

## 8. Практичні інструкції з тестування (Postman & Kafka UI)

### 1. Тестування через Postman

1. Відкрийте файл `docs/postman/notification-service.postman_collection.json`.
2. Запустіть запит **`Kafka Test Order Flow`**:
   - `POST http://localhost:3002/notifications/test-flow/order-101`
3. У відповіді повертається:

   ```json
   {
     "message": "Events for order order-101 successfully sent",
     "statuses": ["CREATED", "PAID", "SHIPPED"]
   }
   ```

### 2. Моніторинг у терміналі NestJS

```text
[Kafka Consumer] 📥 Received event: Partition: 1 | Offset: 0 | Key: order-101 | Status: CREATED
[Kafka Consumer] 📥 Received event: Partition: 1 | Offset: 1 | Key: order-101 | Status: PAID
[Kafka Consumer] 📥 Received event: Partition: 1 | Offset: 2 | Key: order-101 | Status: SHIPPED
```

### 3. Аудит через вебінтерфейс Kafka UI

1. Відкрийте **`http://localhost:8080`**.
2. Перейдіть до кластера `local-cluster` ➡️ розділ **Topics** ➡️ **`order.status-changed`**.
3. У вкладці **Messages** ви побачите детальний розподіл за партиціями та ключами:
   - `order-777` $\rightarrow$ `Partition 0`
   - `order-999` $\rightarrow$ `Partition 0`
   - `order-101` $\rightarrow$ `Partition 1`
   - Окремі оффсети всередині кожної партиції йдуть строго послідовно: `0, 1, 2...`
