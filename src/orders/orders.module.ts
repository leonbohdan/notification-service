import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { OrdersService } from './orders.service.js';
import { OrdersController } from './orders.controller.js';
import { Order } from './entities/order.entity.js';
import { OutboxEvent } from './entities/outbox-event.entity.js';
import { OutboxProcessorWorker } from './workers/outbox-processor.worker.js';

@Module({
  imports: [
    TypeOrmModule.forFeature([Order, OutboxEvent]),
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'orders-outbox-worker',
            brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
          },
          producer: {
            allowAutoTopicCreation: true,
          },
        },
      },
    ]),
  ],
  controllers: [OrdersController],
  providers: [OrdersService, OutboxProcessorWorker],
  exports: [OrdersService],
})
export class OrdersModule {}
