import {
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Logger,
} from '@nestjs/common';
import { Redis } from 'ioredis';

@Injectable()
export class NotificationWorkerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(NotificationWorkerService.name);
  private redisClient: Redis;
  private isRunning = false;

  onApplicationBootstrap() {
    this.initRedisAndStartWorker();
  }

  private async initRedisAndStartWorker() {
    const host = process.env.REDIS_HOST || 'localhost';
    const port = Number(process.env.REDIS_PORT) || 6379;

    this.redisClient = new Redis({
      host,
      port,
      lazyConnect: true,
    });

    try {
      await this.redisClient.connect();
      this.logger.log(` Connected to Redis at ${host}:${port}`);
      this.isRunning = true;
      // Starting the background loop (without await to not block the HTTP server start!)
      this.processQueue();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Failed to connect to Redis: ${message}`);
    }
  }

  private async processQueue() {
    const queueName =
      process.env.NOTIFICATION_QUEUE_NAME || 'queue:notifications';
    this.logger.log(
      `🚀 Notification Worker started, listening to "${queueName}"...`,
    );

    while (this.isRunning) {
      try {
        // BRPOP blocks the connection until an element appears (0 = infinite timeout)
        const result = await this.redisClient.brpop(queueName, 0);

        if (result) {
          const [_queue, payload] = result;
          const task = JSON.parse(payload);

          this.logger.log(
            `[Worker] Processing notification for order #${task.orderId} to ${task.customerEmail}`,
          );

          // Simulating email sending (for example, 2 seconds)
          await new Promise((resolve) => setTimeout(resolve, 2000));

          this.logger.log(
            `[Worker] ✅ Notification for order #${task.orderId} successfully sent!`,
          );
        }
      } catch (err) {
        if (!this.isRunning) break;
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(`[Worker Error] ${message}`);

        // Short pause before the next attempt in case of connection failure
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  onApplicationShutdown() {
    this.isRunning = false;
    if (this.redisClient) {
      this.redisClient.disconnect();
    }
  }
}
