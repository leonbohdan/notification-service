# День 11: Event-Driven — Черги повідомлень з RabbitMQ (AMQP)

Сьогодні ми переводимо взаємодію між нашими мікросервісами на надійні асинхронні рейки за допомогою брокера повідомлень **RabbitMQ**. Ми розберемося з концепціями протоколу AMQP 0-9-1 (Exchanges, Bindings, Queues), налаштуємо асинхронну передачу подій між `order-service` та `notification-service`, навчимося керувати підтвердженням доставки (ACK/NACK) та утилізувати збійні повідомлення у Dead Letter Queue (DLQ).

---

## ⏱️ Розклад Дня 11 (6 годин)

| Блок       | Тривалість | Тема                           | Опис                                                                                        |
| :--------- | :--------- | :----------------------------- | :------------------------------------------------------------------------------------------ |
| **Блок 1** | 1 год      | Алгоритмічний розігрів (TS/JS) | Пріоритетна черга (`PriorityQueue`) на базі бінарної купи (Binary Heap) за $O(\log N)$      |
| **Блок 2** | 2.5 год    | RabbitMQ у Docker та NestJS    | Розгортання брокера, підключення `@nestjs/microservices` (транспорт RMQ), Management UI     |
| **Блок 3** | 1.5 год    | Exchanges, ACK/NACK та DLQ     | Обмінники (`Direct`, `Topic`, `Fanout`), ручне підтвердження та перенаправлення збоїв у DLQ |
| **Блок 4** | 1 год      | Рев'ю та інтерв'ю-підготовка   | Backpressure, `prefetch_count`, патерн Competing Consumers, безпека Poison Messages         |

---

## 🛠️ Завдання 1: Розгортання RabbitMQ та підключення NestJS Microservices

### Мета

Розгорнути RabbitMQ 3.13 з вебінтерфейсом керування у Docker Compose, інтегрувати транспортний рівень RabbitMQ у `order-service` (продюсер подій) та `notification-service` (споживач).

### Кроки реалізації

1. **Додавання RabbitMQ до `docker-compose.microservices.yml`:**

   ```yaml
   rabbitmq:
     image: rabbitmq:3.13-management-alpine
     container_name: rabbitmq_broker
     ports:
       - '5672:5672' # AMQP протокол для сервісів
       - '15672:15672' # Management Web UI
     environment:
       RABBITMQ_DEFAULT_USER: guest
       RABBITMQ_DEFAULT_PASS: guest
     healthcheck:
       test: ['CMD', 'rabbitmq-diagnostics', 'check_port_connectivity']
       interval: 5s
       timeout: 5s
       retries: 5
     networks:
       - microservices_net
   ```

2. **Встановлення клієнтських бібліотек у NestJS:**

   ```bash
   npm install @nestjs/microservices amqplib amqp-connection-manager
   ```

3. **Налаштування Продюсера (`order-service`):**
   У модулі замовлень `order.module.ts` зареєструй мікросервісний клієнт:

   ```typescript
   import { ClientsModule, Transport } from '@nestjs/microservices';

   @Module({
     imports: [
       ClientsModule.register([
         {
           name: 'RABBITMQ_ORDER_SERVICE',
           transport: Transport.RMQ,
           options: {
             urls: [
               process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
             ],
             queue: 'orders_queue',
             queueOptions: {
               durable: true,
             },
           },
         },
       ]),
     ],
     // ...
   })
   export class OrdersModule {}
   ```

   При створенні замовлення відправляй подію через патерн `Event` (без очікування відповіді, Fire-and-Forget):

   ```typescript
   this.client.emit('order_created', {
     orderId: order.id,
     customerEmail: order.customerEmail,
     totalPrice: order.totalPrice,
     createdAt: new Date(),
   });
   ```

4. **Налаштування Споживача (`notification-service`):**
   У `main.ts` сервісу сповіщень ініціалізуй NestJS гібридний мікросервіс:

   ```typescript
   import { NestFactory } from '@nestjs/core';
   import { Transport, MicroserviceOptions } from '@nestjs/microservices';
   import { AppModule } from './app.module';

   async function bootstrap() {
     const app = await NestFactory.create(AppModule);

     app.connectMicroservice<MicroserviceOptions>({
       transport: Transport.RMQ,
       options: {
         urls: [
           process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
         ],
         queue: 'orders_queue',
         noAck: false, // ВАЖЛИВО: вимикаємо автоматичний ACK
         queueOptions: {
           durable: true,
         },
       },
     });

     await app.startAllMicroservices();
     await app.listen(3002);
     console.log('Notification Microservice is listening to RabbitMQ...');
   }
   bootstrap();
   ```

---

## 📦 Завдання 2: Ручні підтвердження (ACK/NACK), типи Exchanges та Dead Letter Queue

### Мета

Гарантувати, що жодне повідомлення не загубиться при падінні воркера, а некоректні ("отруйні") повідомлення не заблокують обробку черги завдяки механізму Dead Letter Queue (DLQ).

### Кроки реалізації

1. **Конфігурація Dead Letter Queue (DLQ) на рівні черги:**
   Якщо повідомлення відхиляється (`nack` або `reject` без рек'юінгу), воно автоматично перенаправляється в обмінник `orders.dlx`:
   - `x-dead-letter-exchange`: `'orders.dlx'`
   - `x-dead-letter-routing-key`: `'orders.dead_letter'`

2. **Реалізація контролера з ручним `ACK` / `NACK`:**
   У `notification.controller.ts`:

   ```typescript
   import { Controller } from '@nestjs/common';
   import {
     EventPattern,
     Payload,
     Ctx,
     RmqContext,
   } from '@nestjs/microservices';

   @Controller()
   export class NotificationConsumerController {
     @EventPattern('order_created')
     async handleOrderCreated(
       @Payload() data: any,
       @Ctx() context: RmqContext,
     ) {
       const channel = context.getChannelRef();
       const originalMsg = context.getMessage();

       try {
         console.log(
           `[Notification Consumer] Обробка події order_created:`,
           data.orderId,
         );

         // Симуляція перевірки валідності email
         if (!data.customerEmail || !data.customerEmail.includes('@')) {
           throw new Error(`Invalid email address: ${data.customerEmail}`);
         }

         // Імітація надсилання email
         await this.sendNotificationEmail(data);

         // 1. УСПІХ: Підтверджуємо повідомлення в брокері
         channel.ack(originalMsg);
         console.log(
           `[Notification Consumer] Успішний ACK для замовлення ${data.orderId}`,
         );
       } catch (error) {
         console.error(
           `[Notification Consumer] Помилка обробки. Відхилення повідомлення...`,
           error,
         );

         // 2. ПОМИЛКА: Відхиляємо повідомлення.
         // Перший boolean (allUpTo): false (тільки це повідомлення)
         // Другий boolean (requeue): false -> ВАЖЛИВО! Встановлюємо false,
         // щоб повідомлення не зациклилося, а пішло прямо в Dead Letter Queue!
         channel.nack(originalMsg, false, false);
       }
     }

     private async sendNotificationEmail(data: any): Promise<void> {
       // симуляція мережевого запиту
       await new Promise((resolve) => setTimeout(resolve, 100));
     }
   }
   ```

3. **Тестування сценаріїв у RabbitMQ Management UI:**
   - Відкрий `http://localhost:15672` (guest/guest).
   - Відправ валідне замовлення: спостерігай зростання швидкості `Ack rate` і відсутність накопичення у черзі.
   - Відправ невалідне замовлення (без email): переконайся, що повідомлення потрапило в чергу `orders.dead_letter` для подальшого ручного аудиту інженерами.
