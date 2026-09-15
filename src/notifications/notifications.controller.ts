import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { NotificationsService } from './notifications.service.js';
import { CreateNotificationDto } from './dto/create-notification.dto.js';
import { UpdateNotificationDto } from './dto/update-notification.dto.js';
import {
  Ctx,
  EventPattern,
  MessagePattern,
  Payload,
  RmqContext,
  KafkaContext,
} from '@nestjs/microservices';

import { IdempotencyStore } from '../common/idempotency.store.js';

@Controller('notifications')
export class NotificationsController {
  private readonly idempotencyStore = new IdempotencyStore(120_000); // TTL 2 min

  constructor(private readonly notificationsService: NotificationsService) {}

  @EventPattern<string>('order_created')
  async handleOrderCreated(@Payload() data: any, @Ctx() context: RmqContext) {
    const channel = context.getChannelRef();
    const originalMsg = context.getMessage();

    try {
      console.log(
        `[Notification Consumer] Handle event order_created: ${JSON.stringify(data)}`,
      );

      if (!data?.customerEmail || !data.customerEmail.includes('@')) {
        throw new Error(`Invalid email: ${data?.customerEmail}`);
      }

      await new Promise((resolve) => setTimeout(resolve, 500));

      channel.ack(originalMsg);
      console.log(
        `[Notification Consumer] ✅ Success ACK for order #${data.orderId}`,
      );
    } catch (error) {
      console.error(
        `[Notification Consumer] ❌ Error:`,
        (error as Error).message,
      );

      channel.nack(originalMsg, false, false);
    }
  }

  @MessagePattern('order.status-changed')
  async handleOrderStatusChanged(
    @Payload() message: any,
    @Ctx() context: KafkaContext,
  ) {
    const rawMsg = context.getMessage();
    const partition = context.getPartition();
    const offset = rawMsg.offset;
    const key = rawMsg.key?.toString();
    // 1. Forming a unique SHA-256 fingerprint of the event
    const eventFingerprint = this.idempotencyStore.generateHash({
      key,
      payload: message,
    });
    // 2. Checking for idempotency: if already processed, ignore duplicate
    if (this.idempotencyStore.has(eventFingerprint)) {
      console.warn(
        `[Kafka Consumer] ⚠️ [DUPLICATE SKIPED] Event for order ${key} has already been processed! (Hash: ${eventFingerprint.slice(0, 8)}...)`,
      );
      return;
    }
    // 3. Fix event duplication: save event to Idempotency Store
    this.idempotencyStore.set(eventFingerprint);
    console.log(
      `[Kafka Consumer] 📥 [FIRST PROCESSING] Partition: ${partition} | Offset: ${offset} | Key: ${key}`,
    );
    // 4. Process event
    await this.notificationsService.create({
      orderId: key || message?.orderId,
      customerEmail: message?.customerEmail || 'customer@example.com',
      message: `Order #${key} on sum ${message?.totalPrice} uah successfully accepted!`,
    });
  }

  @Post('test-flow/:orderId')
  async testFlow(@Param('orderId') orderId: string) {
    return this.notificationsService.testOrderFlow(orderId);
  }

  @Post('send')
  create(@Body() createNotificationDto: CreateNotificationDto) {
    return this.notificationsService.create(createNotificationDto);
  }

  @Get()
  findAll() {
    return this.notificationsService.findAll();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.notificationsService.findOne(+id);
  }

  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body() updateNotificationDto: UpdateNotificationDto,
  ) {
    return this.notificationsService.update(+id, updateNotificationDto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.notificationsService.remove(+id);
  }
}
