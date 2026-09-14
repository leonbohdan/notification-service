# 📘 Підсумки та Архітектурний Звіт: Завдання 1 & 2 (День 11)
## Event-Driven Architecture, RabbitMQ (AMQP 0-9-1), Manual ACK/NACK та Dead Letter Queue

---

## 📑 Зміст

1. [Огляд виконаних робіт (Executive Summary)](#1-огляд-виконаних-робіт-executive-summary)
2. [Анатомія протоколу AMQP 0-9-1 та інтуїція роботи RabbitMQ](#2-анатомія-протоколу-amqp-0-9-1-та-інтуїція-роботи-rabbitmq)
   - [2.1. Життєва метафора: Сортувальний термінал (Нова Пошта)](#21-життєва-метафора-сортувальний-термінал-нова-пошта)
   - [2.2. Що насправді відбувається під капотом під час підключення NestJS](#22-що-насправді-відбувається-під-капотом-під-час-підключення-nestjs)
   - [2.3. Схема AMQP та основні типи Exchanges](#23-схема-amqp-та-основні-типи-exchanges)
3. [Чи всі запити проходять через чергу? Синхронний HTTP vs Асинхронний RabbitMQ](#3-чи-всі-запити-проходять-через-чергу-синхронний-http-vs-асинхронний-rabbitmq)
   - [3.1. Життєвий цикл замовлення (Sequence Diagram)](#31-життєвий-цикл-замовлення-sequence-diagram)
   - [3.2. Порівняльна таблиця: Синхронний HTTP vs Черга RabbitMQ](#32-порівняльна-таблиця-синхронний-http-vs-черга-rabbitmq)
   - [3.3. Навіщо в notification-service одночасно HTTP та RabbitMQ?](#33-навіщо-в-notification-service-одночасно-http-та-rabbitmq)
4. [Надійність доставки (Delivery Guarantees) та життєвий цикл повідомлення](#4-надійність-доставки-delivery-guarantees-та-життєвий-цикл-повідомлення)
5. [Dead Letter Queue (DLQ): Захист від Poison Messages](#5-dead-letter-queue-dlq-захист-від-poison-messages)
6. [Архітектурні особливості та підводні камені (Lessons Learned)](#6-архітектурні-особливості-та-підводні-камені-lessons-learned)
7. [Блок 4: Відповіді на питання (Q&A)](#7-блок-4-відповіді-на-питання-qa)
8. [Алгоритмічний блок: Priority Queue на базі Binary Heap ($O(\log N)$)](#8-алгоритмічний-блок-priority-queue-на-базі-binary-heap-olog-n)
9. [Шпаргалка корисних команд (RabbitMQ Cheat Sheet)](#9-шпаргалка-корисних-команд-rabbitmq-cheat-sheet)

---

## 1. Огляд виконаних робіт (Executive Summary)

У рамках другого дня розробки сервіс сповіщень `notification-service` було переведено з примітивного опитування Redis на промисловий брокер повідомлень **RabbitMQ** за стандартом AMQP 0-9-1:

1. **Інфраструктурне розгортання RabbitMQ 3.13:**
   - Додано сервіс `rabbitmq` на базі `rabbitmq:3.13-management-alpine` у `docker-compose.microservices.yml`.
   - Прокинуто AMQP-порт `5672` (взаємодія між бекендами) та HTTP-порт `15672` (Management Web UI).
   - Інтегровано брокер у спільну ізольовану мережу `microservices_net` із налаштованим healthcheck (`rabbitmq-diagnostics check_port_connectivity`).

2. **Транспортний рівень у NestJS (`@nestjs/microservices`):**
   - Встановлено пакети `@nestjs/microservices`, `amqplib` та менеджер автоматичного відновлення з'єднань `amqp-connection-manager`.
   - Створено **гібридний додаток (Hybrid Application)** у `src/main.ts`, що одночасно приймає звичайні HTTP REST-запити (`app.listen(3002)`) та слухає чергу RabbitMQ через `app.connectMicroservice<MicroserviceOptions>()`.

3. **Гарантія доставки (At-least-once) через Manual ACK/NACK:**
   - Вимкнено автоматичний ACK (`noAck: false`).
   - У `src/notifications/notifications.controller.ts` реалізовано обробник `@EventPattern<string>('order_created')`, де повідомлення підтверджується через `channel.ack(originalMsg)` лише після успішної валідації та емуляції відправки листа.

4. **Механізм захисту від «отруйних» повідомлень (Dead Letter Queue):**
   - Чергу `orders_queue` сконфігуровано з аргументами:
     - `'x-dead-letter-exchange': 'orders.dlx'`
     - `'x-dead-letter-routing-key': 'orders.dead_letter'`
   - У разі виникнення помилки валідації (наприклад, невалідний або відсутній email) викликається `channel.nack(originalMsg, false, false)` з `requeue = false`, що миттєво відправляє некоректне повідомлення в DLQ, не зациклюючи воркер.

---

## 2. Анатомія протоколу AMQP 0-9-1 та інтуїція роботи RabbitMQ

### 2.1. Життєва метафора: Сортувальний термінал (Нова Пошта)

Уявіть, що ваші мікросервіси — це клієнти та кур'єри, а RabbitMQ — сучасний сортувальний термінал:

```
[order-service] ──(Посилка з наліпкою)──> [Exchange: Сортувальник]
                                                    │
                                              (кладе на полицю)
                                                    ▼
                                          [Queue: orders_queue]
                                                    │
                                            (видає кур'єру)
                                                    ▼
                                      [notification-service: Кур'єр]
                                           ├── Успішно? ──> "Підписано в системі" (ACK)
                                           └── Помилка?  ──> У ящик браку (DLQ)
```

1. **`order-service` (Клієнт-відправник):** створив замовлення. Йому не потрібно самому їхати до клієнта з товаром. Він приносить посилку у відділення, клеїть наліпку з ключем маршрутизації (`orders_queue`) і віддає оператору. Робота сервісу замовлень завершена за 2 мілісекунди.
2. **Exchange (Оператор / Сортувальник):** читає наліпку і перекладає посилку у відповідний ящик (чергу `orders_queue`).
3. **Queue (Черга / Полиця на складі):** безпечне сховище, де посилка лежить на диску (`durable: true`) і чекає, поки звільниться кур'єр.
4. **`notification-service` (Кур'єр-споживач):** підключений до відділення. Як тільки з'являється посилка, брокер виштовхує її йому.
5. **ACK (Розписка про доставку):** кур'єр відправив email і передає брокеру: *«Все доставлено!»* (`channel.ack()`). Лише тепер посилка списується з полиці назавжди.
6. **NACK + DLQ (Відділ рекламацій):** кур'єр бачить неіснуючу адресу (немає email). Він каже: *«Доставити неможливо, повертати в чергу не можна»* (`channel.nack(false, false)`). Посилка автоматично летить у ящик браку — **Dead Letter Queue (`orders.dead_letter`)**, де її пізніше вивчать інженери.

---

### 2.2. Що насправді відбувається під капотом під час підключення NestJS

Коли стартує `notification-service`, виконується код:

```typescript
app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.RMQ,
  options: {
    urls: ['amqp://guest:guest@localhost:5672'],
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
await app.startAllMicroservices();
```

Ось що відбувається на мережевому рівні:

1. **TCP Connection (Фізичне з'єднання):**
   - Додаток встановлює TCP-з'єднання з RabbitMQ (`localhost:5672`).
   - Відбувається автентифікація (`guest`/`guest`). Менеджер `amqp-connection-manager` тримає з'єднання відкритим і автоматично відновлює його при збоях мережі.
2. **AMQP Channels (Віртуальні канали):**
   - Відкривати нове TCP-з'єднання під кожну операцію занадто затратно.
   - Тому поверх одного TCP-з'єднання створюються легковажні віртуальні **канали (Channels)** — ізольовані «телефонні лінії» всередині одного мережевого кабелю.
3. **Оголошення черги (`queueDeclare`):**
   - NestJS відправляє брокеру запит: *«Перевір, чи є черга `orders_queue`. Якщо немає — створи її з прапорцем `durable: true` та параметрами DLX»*.
4. **Підписка на події (`basic.consume` — Push-модель):**
   - Сервіс реєструється як активний споживач (`1 consumer`).
   - RabbitMQ сам миттєво виштовхує нові повідомлення у сокет нашого процесу, не вимагаючи від нас постійного опитування в циклі (на відміну від `BRPOP` у Redis).
5. **Маршрутизація всередині NestJS:**
   - Отримавши повідомлення, NestJS десеріалізує JSON, знаходить поле `"pattern": "order_created"`, зіставляє його з декоратором `@EventPattern<string>('order_created')` і викликає відповідний метод контролера `handleOrderCreated(data, context)`.

---

### 2.3. Схема AMQP та основні типи Exchanges

RabbitMQ побудовано за концепцією **«Розумний брокер — Простий споживач» (Smart Broker / Dumb Consumer)**. Продюсер у RabbitMQ **ніколи не надсилає повідомлення безпосередньо в чергу** — він завжди надсилає його в **Обмінник (Exchange)**.

```mermaid
flowchart LR
    P["Producer (order-service)"] -->|"Publish (routingKey: 'orders_queue')"| EX["Exchange (default або custom)"]
    
    subgraph RabbitMQ Broker
        EX -->|"Binding"| Q1[("orders_queue<br/>(durable, DLX config)")]
        
        Q1 -.->|"NACK (requeue=false)"| DLX["DLX: orders.dlx"]
        DLX -->|"Binding ('orders.dead_letter')"| DLQ[("DLQ: orders.dead_letter")]
    end
    
    Q1 -->|"Push (unacked)"| C["Consumer (notification-service)"]
    C -->|"1. Success: channel.ack()"| Q1
    C -->|"2. Fail: channel.nack(false, false)"| Q1
    
    DLQ -->|"Ручний аудит / Dead Letter Worker"| S["Engineer / Alerting"]
```

| Тип Exchange | Механізм маршрутизації | Приклад використання |
| :--- | :--- | :--- |
| **Direct** | Повідомлення надсилається лише в ті черги, де `Binding Key` точно збігається з `Routing Key`. | Точкова доставка задач, Dead Letter Queue (`orders.dlx` ➡️ `orders.dead_letter`). |
| **Fanout** | Широкомовна розсилка (Broadcast): ігнорує Routing Key і копіює повідомлення у **всі** прив'язані черги. | Класичний Pub/Sub: подія `UserRegistered` має піти одночасно у `EmailService`, `AnalyticsService` та `BillingService`. |
| **Topic** | Маршрутизація за шаблонами з масками: `*` (рівно одне слово), `#` (нуль або більше слів). | Складні підписки: події `order.eu.created`, `order.us.cancelled`. Черга сповіщень ЄС слухає `order.eu.*`. |
| **Headers** | Маршрутизація на основі заголовків AMQP (ключ-значення), а не рядка Routing Key. | Специфічні корпоративні сценарії з бінарними атрибутами або комбінацією умов `x-match: all/any`. |

---

## 3. Чи всі запити проходять через чергу? Синхронний HTTP vs Асинхронний RabbitMQ

**Ні, далеко не всі!** Це фундаментальне архітектурне правило розподілених систем: запити чітко розмежовуються за своєю бізнес-природою.

---

### 3.1. Життєвий цикл замовлення (Sequence Diagram)

```mermaid
sequenceDiagram
    autonumber
    actor User as 👤 Покупець (Браузер)
    participant Order as 🏢 order-service
    participant DB as 🗄️ PostgreSQL
    participant RMQ as 🐰 RabbitMQ (orders_queue)
    participant Notif as 📬 notification-service
    actor Email as ✉️ Поштовий сервер

    User->>Order: 1. HTTP POST /orders (Синхронно)
    Note over User,Order: Покупець чекає відповіді: чи успішно створено замовлення?
    
    Order->>DB: 2. Зберегти замовлення в БД (2 мс)
    DB-->>Order: Замовлення #101 збережено!
    
    Order->)RMQ: 3. Кинути подію "order_created" (1 мс)
    Note over Order,RMQ: Fire-and-Forget (кинув і не чекає виконання)
    
    Order-->>User: 4. HTTP 201: "Замовлення оформлено!" (Разом: ~10 мс)
    Note over User: Покупець миттєво бачить екран успіху 🎉

    Note over RMQ,Notif: === Фонова асинхронна робота ===
    RMQ->>Notif: 5. Брокер виштовхує подію у воркер
    Notif->>Email: 6. Відправка email (може тривати 2-5 секунд)
    Email-->>Notif: Лист надіслано!
    Notif->>RMQ: 7. channel.ack()
```

---

### 3.2. Порівняльна таблиця: Синхронний HTTP vs Черга RabbitMQ

| Критерій | Синхронний HTTP REST | Асинхронна черга RabbitMQ |
| :--- | :--- | :--- |
| **Очікування відповіді** | Клієнт **заблокований** і чекає на результат тут і зараз. | Відправник **не чекає** виконання задачі (Fire-and-Forget). |
| **Швидкість для клієнта** | Дорівнює тривалості найповільнішої операції (секунди). | Миттєва (декілька мілісекунд). |
| **Зв'язаність сервісів** | **Жорстка:** якщо `notification-service` впав, клієнт отримує 500 помилку і все замовлення зривається. | **Слабка (Decoupled):** якщо воркер вимкнений, повідомлення безпечно чекають у черзі на диску. |
| **Типові сценарії** | - Авторизація (`POST /login`)<br/>- Отримання даних (`GET /profile`)<br/>- Healthcheck (`GET /health`) | - Відправка Email / SMS / Push<br/>- Генерація важких звітів та PDF<br/>- Синхронізація складських залишків |

---

### 3.3. Навіщо в notification-service одночасно HTTP та RabbitMQ?

Наш `notification-service` спроектований як **гібридний застосунок (Hybrid Application)**:
- **HTTP (порт 3002):** потрібен для службових цілей — `GET /health` (перевірка стану контейнера оркестратором) та прямого синхронного відправлення `POST /notifications/send`, якщо сповіщення критично термінове.
- **RabbitMQ (порт 5672):** слухає чергу `orders_queue` для обробки регулярного потоку фонових подій від іншої частини системи.

---

## 4. Надійність доставки (Delivery Guarantees) та життєвий цикл повідомлення

### Семантика At-least-once Delivery
Коли ми вимикаємо автоматичний ACK (`noAck: false`), брокер чекає підтвердження від нашого застосунку.

```text
[Message in Queue: Ready] 
       │
       ▼ (Відправлено воркеру)
[Message in Queue: Unacked]
       │
       ├──────────────────────────────────────────────┐
       │ channel.ack()                                │ channel.nack(false, false)
       ▼                                              ▼
[Видалено з черги назавжди]              [Перенаправлено в DLX / DLQ]
```

### Чому автоматичний ACK (`noAck: true`) небезпечний у продакшені?
- При `noAck: true` RabbitMQ вважає повідомлення успішно доставленим **у ту саму мікросекунду, коли виштовхнув його в TCP-сокет**.
- Якщо процес Node.js впаде (OOM killer, segmentation fault, раптове перезавантаження контейнера) посеред обробки, **повідомлення втрачається безповоротно**.
- При `noAck: false`, якщо з'єднання розірветься до виклику `channel.ack()`, брокер автоматично поверне повідомлення зі статусу `Unacked` назад у `Ready`, і його підхопить інший живий воркер.

---

## 5. Dead Letter Queue (DLQ): Захист від Poison Messages

### Що таке «Отруйне повідомлення» (Poison Message)?
Це повідомлення із синтаксично чи логічно некоректними даними (наприклад, відсутній email клієнта або пошкоджений JSON). 
- Якщо воркер викине помилку і зробить `channel.nack(msg, false, true)` з прапорцем `requeue = true`, RabbitMQ негайно поверне це повідомлення в голову черги.
- Воркер негайно знову вичитає це повідомлення, знову кине помилку, знову зробить nack...
- **Результат:** Нескінченний шторм ретраїв (Infinite Retry Loop), 100% навантаження на CPU, переповнення дисків логами та повне блокування черги для валідних замовлень.

### Як працює зв'язка DLX + DLQ
1. На рівні черги задаються аргументи:
   - `x-dead-letter-exchange`: назва обмінника для збійних повідомлень (`orders.dlx`).
   - `x-dead-letter-routing-key`: ключ для маршрутизації в чергу помилок (`orders.dead_letter`).
2. Коли воркер фіксує помилку бізнес-валідації, він викликає:
   ```typescript
   channel.nack(originalMsg, false, false); // requeue: false
   ```
3. RabbitMQ бачить `requeue: false`, перехоплює повідомлення і через `orders.dlx` перекладає його в ізольовану чергу `orders.dead_letter`.
4. До повідомлення автоматично додається масив заголовків `x-death`:
   - `reason: "rejected"` (повідомлення було відхилено).
   - `queue: "orders_queue"` (з якої черги воно вилетіло).
   - `time`: точний Unix-таймштамп збою.

---

## 6. Архітектурні особливості та підводні камені (Lessons Learned)

У процесі налаштування та запуску ми зіткнулися з чотирма критичними інженерними нюансами, які обов'язково трапляються на реальних проектах:

### 1. Незмінність конфігурації черги (Queue Immutability)
- **Проблема:** Помилка `406 PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange' for queue 'orders_queue'`.
- **Причина:** Якщо чергу було створено без аргументів, AMQP забороняє змінювати її параметри на льоту.
- **Рішення:** Видалення старої черги (`Delete Queue`) у RabbitMQ UI перед повторним запуском сервісу або використання версіонування імен черг у продакшені (наприклад, `orders_queue_v2`).

### 2. Сувора типізація TypeScript 6 та NestJS 12
- **Проблема:** Помилка компіляції TS1241: `Type 'unknown' is not assignable to type 'RmqContext'`.
- **Причина:** У новій версії NestJS 12 декоратор `@EventPattern` отримав перевантаження для типізованих подій із `...args: unknown[]`, що конфліктує зі `strictFunctionTypes: true`.
- **Рішення:** Явне зазначення дженерика `@EventPattern<string>('order_created')`, що перемикає компілятор на універсальну сигнатуру `MethodDecorator`.

### 3. Конфлікт портів `EADDRINUSE: address already in use :::3002`
- **Проблема:** Локальний `npm run start:dev` впав при виклику `app.listen(3002)`.
- **Причина:** У фоні Docker продовжував працювати старий контейнер `notification-service`, який тримав порт.
- **Рішення:** Зупинка застарілого контейнера (`docker stop notification-service`) для локальної швидкої розробки з Watch Mode.

### 4. Специфіка NestJS RMQ Transport Payload
- **Проблема:** Якщо надіслати в чергу просто сирий JSON `{"orderId": 1}`, NestJS проігнорує його.
- **Причина:** NestJS вимагає обгортку з полями `"pattern"` та `"data"`, за якими внутрішній роутер мікросервісу знаходить потрібний метод-обробник.

---

## 7. Блок 4: Відповіді на питання (Q&A)

### Q1: Що таке Backpressure (зворотний тиск) і як працює `prefetch_count` (QoS) у RabbitMQ?
**Відповідь:**
- За замовчуванням RabbitMQ намагається віддати споживачу **всі** доступні в черзі повідомлення так швидко, як дозволяє мережа (Push-модель).
- Якщо в чергу раптово надійде 50 000 замовлень, брокер виштовхне їх усі в оперативну пам'ять воркера. Node.js не встигне обробити таку кількість асинхронних промісів і впаде з помилкою `JavaScript heap out of memory`.
- **`prefetch_count` (Quality of Service / QoS)** задає жорсткий ліміт: скільки непідтверджених повідомлень (`Unacked`) брокер має право одночасно тримати у даного споживача.
- Наприклад, при `prefetchCount: 10` воркер отримує 10 повідомлень, і наступне (11-те) RabbitMQ надішле лише тоді, коли воркер зробить хоча б один `channel.ack()`. Це і є реалізація **Backpressure** — регулювання швидкості постачання під швидкість обробки.

### Q2: Як працює патерн Competing Consumers і як досягти Fair Dispatch?
**Відповідь:**
- **Competing Consumers (Конкуруючі споживачі):** це запуск кількох однакових екземплярів сервісу (наприклад, 3 репліки `notification-service`), підключених до однієї черги `orders_queue`.
- RabbitMQ за замовчуванням розподіляє повідомлення між ними за принципом **Round-Robin** (по черзі: 1-му, 2-му, 3-му, знову 1-му).
- **Проблема Round-Robin:** якщо непарні замовлення прості (обробка 10 мс), а парні важкі (генерація PDF на 5 секунд), один воркер буде перевантажений, а інші простоюватимуть.
- **Рішення (Fair Dispatch):** комбінація `noAck: false` та `prefetchCount: 1`. У такому разі RabbitMQ віддає воркеру наступне завдання лише тоді, коли той повністю звільнився від попереднього.

### Q3: Порівняння: Redis vs RabbitMQ vs Apache Kafka. Коли що обирати?
**Відповідь:**

| Критерій | Redis (Lists / PubSub) | RabbitMQ (AMQP) | Apache Kafka |
| :--- | :--- | :--- | :--- |
| **Архітектурна модель** | In-Memory сховище даних | **Smart Broker / Dumb Consumer** | **Dumb Broker / Smart Consumer** |
| **Маршрутизація** | Тільки прямі ключі або канали | **Складна** (Exchanges, Topics, Direct, Headers) | Проста (за ключем партиції) |
| **Збереження даних** | До вичитування (`BRPOP` видаляє дані) | До `ACK` (після підтвердження видаляється) | **Незмінний лог (Append-only Log)** зберігається N днів |
| **Повторне читання (Replay)** | Неможливо | Неможливо (якщо не писати в окрему чергу) | **Так**, можна скинути Offset назад |
| **Пропускна здатність** | 100k+ RPS (обмежено RAM) | 20k – 50k RPS | **1 000 000+ RPS** |
| **Ідеальний кейс** | Прості фонові задачі, кеш, швидкі черги без складного роутингу. | **Транзакційні бізнес-події**, банківські операції, складний роутинг, гарантована доставка з DLQ. | **Big Data, аналітика, Event Sourcing**, телеметрія, IoT, аудит-логи. |

### Q4: Чи гарантує RabbitMQ доставку «Exactly-Once»? Як вирішується проблема дублікатів?
**Відповідь:**
- **Ні.** Жодна розподілена черга у світі не може на 100% гарантувати чистий *Exactly-once delivery* у разі збоїв мережі (теорема двох генералів). RabbitMQ гарантує **At-least-once** (щонайменше один раз).
- *Чому виникають дублікати:* воркер успішно виконав дію (наприклад, надіслав SMS), але в момент виклику `channel.ack()` моргнула мережа. Брокер не отримав ACK, повернув повідомлення в чергу, і його вичитав інший воркер.
- **Рішення:** **Ідемпотентність споживача (Idempotent Consumer)**:
  1. Кожне повідомлення повинно мати унікальний `eventId` або `idempotencyKey` (наприклад, `orderId`).
  2. Перед виконанням операції воркер перевіряє в БД / Redis: `SET lock:order:101 "processed" NX EX 86400`.
  3. Якщо ключ уже існує — воркер просто робить `channel.ack()` і пропускає повторну обробку.

---

## 8. Алгоритмічний блок: Priority Queue на базі Binary Heap ($O(\log N)$)

У високонавантажених брокерах повідомлень (таких як RabbitMQ) пріоритетні черги реалізуються на базі **двійкової купи (Binary Heap)**, а не сортованих масивів.

### Чому масив неефективний?
- Якщо тримати несортований масив: додавання $O(1)$, але пошук найвищого пріоритету вимагає повного сканування за $O(N)$.
- Якщо сортувати масив: вставка нового елемента вимагає зсуву елементів за $O(N)$ або повного сортування за $O(N \log N)$.
- **Двійкова купа (Binary Heap):** дає гарантований час $O(\log N)$ на додавання (`push`) і вилучення (`pop`), та $O(1)$ на перегляд максимального/мінімального елемента (`peek`).

### Реалізація на TypeScript:

```typescript
export interface PriorityItem<T> {
  value: T;
  priority: number; // Менше число = вищий пріоритет (Min-Heap)
}

export class PriorityQueue<T> {
  private heap: PriorityItem<T>[] = [];

  // O(1)
  get size(): number {
    return this.heap.length;
  }

  // O(1)
  isEmpty(): boolean {
    return this.heap.length === 0;
  }

  // O(1)
  peek(): T | null {
    return this.isEmpty() ? null : this.heap[0].value;
  }

  // O(log N): додаємо в кінець і просіюємо вгору
  push(value: T, priority: number): void {
    this.heap.push({ value, priority });
    this.siftUp(this.heap.length - 1);
  }

  // O(log N): міняємо корінь з останнім, видаляємо і просіюємо вниз
  pop(): T | null {
    if (this.isEmpty()) return null;
    if (this.size === 1) return this.heap.pop()!.value;

    const root = this.heap[0].value;
    this.heap[0] = this.heap.pop()!;
    this.siftDown(0);
    return root;
  }

  private siftUp(index: number): void {
    let current = index;
    while (current > 0) {
      const parent = Math.floor((current - 1) / 2);
      if (this.heap[current].priority >= this.heap[parent].priority) break;

      this.swap(current, parent);
      current = parent;
    }
  }

  private siftDown(index: number): void {
    let current = index;
    const length = this.heap.length;

    while (true) {
      let smallest = current;
      const left = 2 * current + 1;
      const right = 2 * current + 2;

      if (left < length && this.heap[left].priority < this.heap[smallest].priority) {
        smallest = left;
      }
      if (right < length && this.heap[right].priority < this.heap[smallest].priority) {
        smallest = right;
      }

      if (smallest === current) break;

      this.swap(current, smallest);
      current = smallest;
    }
  }

  private swap(i: number, j: number): void {
    [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
  }
}
```

---

## 9. Шпаргалка корисних команд (RabbitMQ Cheat Sheet)

```bash
# 1. Запуск брокера RabbitMQ у Docker
docker compose -f docker-compose.microservices.yml up -d rabbitmq

# 2. Перегляд статусу та здоров'я брокера
docker exec -it rabbitmq_broker rabbitmq-diagnostics check_port_connectivity

# 3. Список черг та кількість повідомлень через rabbitmqctl
docker exec -it rabbitmq_broker rabbitmqctl list_queues name messages messages_ready messages_unacknowledged

# 4. Список активних споживачів (Consumers)
docker exec -it rabbitmq_broker rabbitmqctl list_consumers

# 5. Очищення черги від накопичених повідомлень (Purge)
docker exec -it rabbitmq_broker rabbitmqctl purge_queue orders_queue

# 6. Видалення черги (корисно при PRECONDITION_FAILED)
docker exec -it rabbitmq_broker rabbitmqadmin delete queue name=orders_queue
```
