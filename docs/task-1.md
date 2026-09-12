# День 10: Event-Driven — Docker Compose Network та Мікросервісна архітектура

Сьогодні ми розпочинаємо третій мікропроєкт — **Event-Driven Microservices Network**. Наша мета — спроєктувати розподілену систему з кількох мікросервісів, об'єднати їх у спільну ізольовану Docker-мережу, на практиці дослідити недоліки та ризики синхронної взаємодії (HTTP Cascading Failures) та підготувати фундамент для впровадження асинхронних шин подій і брокерів повідомлень.

---

## ⏱️ Розклад Дня 10 (6 годин)

| Блок       | Тривалість | Тема                                   | Опис                                                                                          |
| :--------- | :--------- | :------------------------------------- | :-------------------------------------------------------------------------------------------- |
| **Блок 1** | 1 год      | Алгоритмічний розігрів (TS/JS)         | Патерн `Retry with Exponential Backoff and Full Jitter` для стійких мережевих викликів        |
| **Блок 2** | 2.5 год    | Інфраструктура (Docker Compose)        | Об'єднання сервісів у мережу `event_driven_net`, внутрішній DNS та `healthcheck` залежності   |
| **Блок 3** | 1.5 год    | Синхронний REST vs Асинхронний зв'язок | Демонстрація проблеми лавиноподібної відмови (Cascading Failures) та вичерпання пулу з'єднань |
| **Блок 4** | 1 год      | Рев'ю та інтерв'ю-підготовка           | Теорема CAP, розподілені транзакції, патерн Saga (Orchestration vs Choreography)              |

---

## 🛠️ Завдання 1: Об'єднання сервісів у спільну мережу Docker Compose (з Redis)

### Мета

Об'єднати сервіси системи у спільну ізольовану bridge-мережу Docker Compose, додати швидке сховище Redis для кешування та черг, забезпечити взаємний резолвінг імен сервісів через вбудований Docker DNS та налаштувати коректний порядок запуску за допомогою перевірок готовності (`healthcheck`).

### Архітектура системи

1. **`order-service`** (NestJS + PostgreSQL) — сервіс створення та управління замовленнями (порт 3000).
2. **`analytics-service`** (NestJS + MongoDB) — аналітичний сервіс звітів (порт 3001).
3. **`notification-service`** (NestJS / Node.js) — легкий мікросервіс обробки та надсилання сповіщень (порт 3002).
4. **`redis`** (In-Memory Data Store) — швидкий буфер обміну подіями / черга завдань та сховище кешу (порт 6379).

```
         +-----------------------------------------------------------------------+
         |                   Docker Network: microservices_net                   |
         |                                                                       |
         |   +---------------+                    +--------------------------+   |
         |   | order-service | -----------------> | analytics-service        |   |
         |   +---------------+    (HTTP/REST)     +--------------------------+   |
         |      |         |                       |         MongoDB          |   |
         |      |         |                       +--------------------------+   |
         |      |         | (Async Queue: LPUSH)                                 |
         |      |         v                                                      |
         |      |      +-------+                                                 |
         |      |      | Redis | <--------------------+                          |
         |      |      +-------+                      |                          |
         |      | (HTTP Fallback / Synchronous)       |                          |
         |      v                                     |                          |
         |   +----------------------+                 | (Worker BRPOP)           |
         |   | notification-service | ----------------+                          |
         |   +----------------------+                                            |
         +-----------------------------------------------------------------------+
```

### Кроки реалізації

1. **Створення сервісу `notification-service`:**
   Створи мінімальний NestJS/Express додаток для обробки сповіщень:
   - **HTTP-ендпоінт:** `POST /notifications/send` $\rightarrow$ приймає `{ orderId, customerEmail, message }`, логує відправку в консоль та повертає `{ status: 'SENT', sentAt: new Date() }`.
   - **Redis Worker:** фоновий модуль/сервіс, готовий приймати завдання з черги Redis (буде задіяний у Завданні 3).

2. **Написання загального `docker-compose.microservices.yml`:**
   Створи файл оркестрації в корені проєкту з підключенням Redis:

   ```yaml
   version: "3.8"

   networks:
     microservices_net:
       driver: bridge

   services:
     postgres-db:
       image: postgres:16-alpine
       environment:
         POSTGRES_USER: user
         POSTGRES_PASSWORD: password
         POSTGRES_DB: order_db
       healthcheck:
         test: ["CMD-SHELL", "pg_isready -U user -d order_db"]
         interval: 5s
         timeout: 5s
         retries: 5
       networks:
         - microservices_net

     mongodb:
       image: mongo:7.0
       healthcheck:
         test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
         interval: 5s
         timeout: 5s
         retries: 5
       networks:
         - microservices_net

     redis:
       image: redis:7-alpine
       container_name: redis
       ports:
         - "6379:6379"
       healthcheck:
         test: ["CMD", "redis-cli", "ping"]
         interval: 5s
         timeout: 3s
         retries: 5
       networks:
         - microservices_net

     notification-service:
       build:
         context: ./notification-service
         dockerfile: Dockerfile
       ports:
         - "3002:3002"
       environment:
         REDIS_HOST: redis
         REDIS_PORT: 6379
       depends_on:
         redis:
           condition: service_healthy
       networks:
         - microservices_net

     analytics-service:
       build:
         context: ./analytics-service
         dockerfile: Dockerfile
       ports:
         - "3001:3001"
       environment:
         MONGO_URI: mongodb://mongodb:27017/insight_pulse
       depends_on:
         mongodb:
           condition: service_healthy
       networks:
         - microservices_net

     order-service:
       build:
         context: ./order-service
         dockerfile: Dockerfile
       ports:
         - "3000:3000"
       environment:
         DATABASE_URL: postgresql://user:password@postgres-db:5432/order_db
         ANALYTICS_SERVICE_URL: http://analytics-service:3001
         NOTIFICATION_SERVICE_URL: http://notification-service:3002
         REDIS_HOST: redis
         REDIS_PORT: 6379
       depends_on:
         postgres-db:
           condition: service_healthy
         redis:
           condition: service_healthy
       networks:
         - microservices_net
   ```

3. **Перевірка DNS-резолвінгу та зв'язку:**
   Запусти систему: `docker compose -f docker-compose.microservices.yml up -d`.
   Зайди всередину контейнера `order-service`:

   ```bash
   docker exec -it <order_container_id> sh
   # 1. Перевір зв'язок за назвами сервісів замість localhost:
   ping notification-service
   curl http://analytics-service:3001/health

   # 2. Перевір доступність Redis через вбудований DNS Docker:
   nc -zv redis 6379
   # або використовуючи redis-cli (якщо встановлено):
   redis-cli -h redis ping
   ```

---

## 💥 Завдання 2: Анатомія лавиноподібної відмови (Cascading Failure) та захист через асинхронний буфер Redis

### Мета

1. На практиці відтворити та проаналізувати головний фатальний недолік синхронних HTTP-запитів: блокування пулу з'єднань, деградація чуйності всієї системи та відмова основного бізнес-процесу при збої другорядного сервісу.
2. Продемонструвати розв'язання цієї проблеми через асинхронне буферизування задач у **Redis** (патерн _Producer-Consumer / Task Queue_ на базі команд `LPUSH` і `BRPOP`), що виступає концептуальним містком до спеціалізованих брокерів повідомлень (RabbitMQ, Kafka).

### Кроки експерименту

#### Частина А: Відтворення синхронної лавиноподібної відмови

1. **Реалізація синхронного ланцюжка у `order-service`:**
   При виклику `POST /orders` (створення замовлення):
   - Крок 1: Замовлення зберігається в базі даних PostgreSQL.
   - Крок 2: Синхронний виклик `HttpService.post('http://analytics-service:3001/events', ...)` для реєстрації події аналітики.
   - Крок 3: Синхронний виклик `HttpService.post('http://notification-service:3002/notifications/send', ...)` для відправки email клієнту.
   - Крок 4: Повернення клієнту статусу `201 Created`.

2. **Моделювання збою інфраструктури:**
   - **Сценарій А (Повільний сервіс сповіщень):** Додай у `notification-service` штучну затримку в 10 секунд (`await sleep(10000)`).
     - Запусти навантажувальний тест на `POST /orders` (наприклад, 50 запитів через `autocannon` або Apache Benchmark: `ab -n 50 -c 10 http://localhost:3000/orders`).
     - Зафіксуй поведінку: час створення замовлення для користувача злітає до 10+ секунд, черга HTTP socket'ів Node.js переповнюється, пам'ять росте.
   - **Сценарій Б (Повне падіння сервісу сповіщень):** Зупини контейнер `notification-service` (`docker stop <notification_container_id>`).
     - Відправ `POST /orders`.
     - Зафіксуй критичний збій: клієнт отримує помилку `500 Internal Server Error`, замовлення не створено (або створено в БД, але транзакція відкотилася / клієнт отримав помилку), бізнес втрачає конверсію через падіння необов'язкового сповіщення!

---

#### Частина Б: Усунення відмови за допомогою черги задач на Redis (Producer-Consumer)

1. **Рефакторинг `order-service` (Producer):**
   - Усунь прямий HTTP-виклик до `notification-service` з життєвого циклу створення замовлення.
   - Підключи клієнт Redis (`ioredis` або `redis`).
   - Замість очікування HTTP-відповіді, надсилай подію створення замовлення в список Redis:

     ```typescript
     // Миттєве скидання задачі в буфер пам'яті (~1-3 мс)
     await redisClient.lpush(
       "queue:notifications",
       JSON.stringify({
         orderId: order.id,
         customerEmail: order.customerEmail,
         message: "Your order was created!",
       }),
     );
     ```

   - Повертай клієнту статус `201 Created` негайно після фіксації в PostgreSQL та відправки в Redis.

2. **Реалізація воркера в `notification-service` (Consumer):**
   - Створи безперервний фоновий цикл із блокуючим вичитуванням черги Redis:

     ```typescript
     async function startNotificationWorker(redisClient: Redis) {
       console.log("🚀 Notification Worker запущено, очікування задач...");
       while (true) {
         try {
           // BRPOP блокує з'єднання до появи елемента, не навантажуючи CPU (timeout 0 = нескінченно)
           const result = await redisClient.brpop("queue:notifications", 0);
           if (result) {
             const [_queue, payload] = result;
             const task = JSON.parse(payload);
             console.log(
               `[Worker] Обробка сповіщення для замовлення #${task.orderId}`,
             );
             // Симуляція відправки (наприклад, 2-3 секунди або навіть 10 секунд)
             await sleep(2000);
             console.log(
               `[Worker] ✅ Сповіщення для #${task.orderId} успішно надіслано на ${task.customerEmail}`,
             );
           }
         } catch (err) {
           console.error("[Worker Error]", err);
           await sleep(1000);
         }
       }
     }
     ```

3. **Контрольне тестування стійкості:**
   - **Перевірка швидкодії:** Навіть якщо воркер обробляє повідомлення 10 секунд, клієнт отримує відповідь `POST /orders` за 5–20 мілісекунд!
   - **Перевірка стійкості до відмов:**
     1. Зупини `notification-service`: `docker stop <notification_container_id>`.
     2. Відправ 10 запитів `POST /orders`. Усі вони повертають `201 Created` без жодної помилки!
     3. Перевір стан черги в Redis: `redis-cli -h localhost -p 6379 llen queue:notifications` (покаже 10 задач у буфері).
     4. Запусти сервіс сповіщень: `docker start <notification_container_id>`.
     5. Спостерігай у логах, як воркер спокійно вичитує та обробляє всі накопичені сповіщення без втрати даних!

---

#### Частина В: Архітектурний аналіз

Сформулюй висновки:

1. Чому другорядні операції (аналітика, сповіщення, аудит) категорично **не повинні** виконуватися у синхронному життєвому циклі основного бізнес-запиту.
2. Які переваги дає асинхронний буфер на Redis порівняно з REST HTTP (відв'язування сервісів у часі, згладжування пікових навантажень — _Spike Arresting_).
3. Чому для великих enterprise-систем згодом потрібні повноцінні брокери (RabbitMQ / Kafka) замість простого Redis List: потреба в надійному підтвердженні доставки (`ACK`/`NACK`), Dead Letter Queue (DLQ), розгалуженні потоків (_Pub/Sub Fanout_ на багатьох незалежних підписників) та збереженні історії подій.
