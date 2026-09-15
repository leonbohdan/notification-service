import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { CreateOrderDto } from './dto/create-order.dto.js';
import { Order } from './entities/order.entity.js';
import { OutboxEvent, OutboxStatus } from './entities/outbox-event.entity.js';

@Injectable()
export class OrdersService {
  constructor(private readonly dataSource: DataSource) {}

  async createOrder(createOrderDto: CreateOrderDto) {
    // Відкриваємо єдину атомарну транзакцію в PostgreSQL
    return this.dataSource.transaction(async (manager) => {
      // 1. Створюємо і зберігаємо сутність Замовлення
      const order = manager.create(Order, {
        customerEmail: createOrderDto.customerEmail,
        totalPrice: createOrderDto.totalPrice,
        status: 'CREATED',
      });
      const savedOrder = await manager.save(Order, order);

      // 2. У ТІЙ САМІЙ ТРАНЗАКЦІЇ зберігаємо подію в таблицю outbox_events!
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

      // Якщо на цьому моменті вимкнеться живлення — нічого не запишеться (Rollback).
      // Якщо запишеться — то гарантовано обидва записи (Commit)!
      return savedOrder;
    });
  }
}
