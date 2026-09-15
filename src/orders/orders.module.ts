import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OrdersService } from './orders.service.js';
import { OrdersController } from './orders.controller.js';
import { Order } from './entities/order.entity.js';
import { OutboxEvent } from './entities/outbox-event.entity.js';

@Module({
  imports: [TypeOrmModule.forFeature([Order, OutboxEvent])],
  controllers: [OrdersController],
  providers: [OrdersService],
  exports: [OrdersService],
})
export class OrdersModule {}
