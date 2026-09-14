import { Injectable, Inject, OnModuleInit } from '@nestjs/common';
import { CreateNotificationDto } from './dto/create-notification.dto.js';
import { UpdateNotificationDto } from './dto/update-notification.dto.js';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class NotificationsService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_PRODUCER_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  async publishOrderStatusEvent(
    orderId: string,
    status: string,
    payload: any = {},
  ) {
    return this.kafkaClient.emit('order.status-changed', {
      key: orderId,
      value: {
        orderId,
        status,
        payload,
        timestamp: new Date().toISOString(),
      },
    });
  }

  async testOrderFlow(orderId: string) {
    const statuses = ['CREATED', 'PAID', 'SHIPPED'];
    for (const status of statuses) {
      await this.publishOrderStatusEvent(orderId, status, { amount: 250 });
    }
    return {
      message: `Events for order ${orderId} successfully sent`,
      statuses,
    };
  }

  async create(createNotificationDto: CreateNotificationDto) {
    console.log(
      `[Notification] 📩 Creating notification for order #${createNotificationDto.orderId} to ${createNotificationDto.customerEmail}: "${createNotificationDto.message}"`,
    );

    // await new Promise((resolve) => setTimeout(resolve, 10000)); // 10 seconds delay

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
