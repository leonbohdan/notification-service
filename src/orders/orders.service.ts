import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { CreateOrderDto } from './dto/create-order.dto.js';
import { Order } from './entities/order.entity.js';
import { OutboxEvent, OutboxStatus } from './entities/outbox-event.entity.js';

@Injectable()
export class OrdersService {
  constructor(private readonly dataSource: DataSource) {}

  async createOrder(createOrderDto: CreateOrderDto) {
    // Open one single atomic transaction in PostgreSQL
    return this.dataSource.transaction(async (manager) => {
      // 1. Create and save the Order entity
      const order = manager.create(Order, {
        customerEmail: createOrderDto.customerEmail,
        totalPrice: createOrderDto.totalPrice,
        status: 'CREATED',
      });
      const savedOrder = await manager.save(Order, order);

      // 2. Save the event to the outbox_events table in the same transaction!
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

      // If power goes out at this point - nothing will be written (Rollback).
      // If it is written - then both records are guaranteed (Commit)!
      return savedOrder;
    });
  }
}
