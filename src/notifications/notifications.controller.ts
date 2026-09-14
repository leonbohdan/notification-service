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
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';

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
