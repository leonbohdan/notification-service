import { Injectable, Logger, Inject, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { DataSource } from 'typeorm';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity.js';

@Injectable()
export class OutboxProcessorWorker implements OnModuleInit {
  private readonly logger = new Logger(OutboxProcessorWorker.name);
  private isRunning = false;

  constructor(
    private readonly dataSource: DataSource,
    @Inject('KAFKA_PRODUCER_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  // Poll every 2 seconds
  @Cron('*/2 * * * * *')
  async processOutboxMessages() {
    // Prevent overlapping polls if the previous batch is still being processed
    if (this.isRunning) return;
    this.isRunning = true;

    try {
      await this.dataSource.transaction(async (manager) => {
        // 1. SKIP LOCKED
        const pendingEvents = await manager
          .createQueryBuilder(OutboxEvent, 'event')
          .setLock('pessimistic_write')
          .setOnLocked('skip_locked')
          .where('event.status = :status', { status: OutboxStatus.PENDING })
          .orderBy('event.createdAt', 'ASC')
          .take(20)
          .getMany();

        if (pendingEvents.length === 0) return;

        this.logger.log(
          `Found ${pendingEvents.length} Outbox events to publish`,
        );

        for (const event of pendingEvents) {
          try {
            // 2. Publish to Kafka broker (topic 'order.status-changed')
            await firstValueFrom(
              this.kafkaClient.emit('order.status-changed', {
                key: event.aggregateId, // Partition Key for ordering guarantee!
                value: event.payload,
              }),
            );

            // 3. Success -> update status to PUBLISHED
            event.status = OutboxStatus.PUBLISHED;
            event.processedAt = new Date();
            await manager.save(OutboxEvent, event);

            this.logger.log(
              `Event ${event.id} (${event.eventType}) successfully delivered to broker!`,
            );
          } catch (err: any) {
            this.logger.error(
              `Error sending event ${event.id}: ${err.message}`,
            );
            event.retryCount += 1;
            event.errorMessage = err.message;
            if (event.retryCount >= 5) {
              event.status = OutboxStatus.FAILED;
            }
            await manager.save(OutboxEvent, event);
          }
        }
      });
    } finally {
      this.isRunning = false;
    }
  }
}
