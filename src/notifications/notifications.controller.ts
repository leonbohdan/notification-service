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

@Controller('notifications')
export class NotificationsController {
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
    const key = rawMsg.key?.toString(); // Key as buffer in Kafka

    console.log(
      `[Kafka Consumer] 📥 Received event:`,
      `Partition: ${partition} | Offset: ${offset} | Key: ${key} | Status: ${message?.status}`,
    );

    // Here you can call notificationsService or save logic if needed
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
