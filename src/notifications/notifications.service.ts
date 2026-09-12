import { Injectable } from '@nestjs/common';
import { CreateNotificationDto } from './dto/create-notification.dto.js';
import { UpdateNotificationDto } from './dto/update-notification.dto.js';

@Injectable()
export class NotificationsService {
  create(createNotificationDto: CreateNotificationDto) {
    console.log(
      `[Notification] 📩 Creating notification for order #${createNotificationDto.orderId} to ${createNotificationDto.customerEmail}: "${createNotificationDto.message}"`,
    );

    return {
      status: 'SENT',
      orderId: createNotificationDto.orderId,
      sentAt: new Date().toISOString(),
    };
  }

  findAll() {
    return `This action returns all notifications`;
  }

  findOne(id: number) {
    return `This action returns a #${id} notification`;
  }

  update(id: number, updateNotificationDto: UpdateNotificationDto) {
    return `This action updates a #${id} notification`;
  }

  remove(id: number) {
    return `This action removes a #${id} notification`;
  }
}
