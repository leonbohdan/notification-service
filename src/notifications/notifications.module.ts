import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { NotificationsService } from './notifications.service.js';
import { NotificationsController } from './notifications.controller.js';
import { NotificationWorkerService } from './notification-worker.service.js';

@Module({
  imports: [
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
    ]),
  ],
  controllers: [NotificationsController],
  providers: [NotificationsService, NotificationWorkerService],
})
export class NotificationsModule {}
