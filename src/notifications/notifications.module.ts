import { Module } from '@nestjs/common';
import { NotificationsService } from './notifications.service.js';
import { NotificationsController } from './notifications.controller.js';
import { NotificationWorkerService } from './notification-worker.service.js';

@Module({
  controllers: [NotificationsController],
  providers: [NotificationsService, NotificationWorkerService],
})
export class NotificationsModule {}
