import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';

describe('AppController', () => {
  let appController: AppController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [AppService],
    }).compile();

    appController = app.get<AppController>(AppController);
  });

  describe('health', () => {
    it('should return health status', () => {
      const result = appController.healthCheck();
      expect(result).toHaveProperty('status', 'ok');
      expect(result).toHaveProperty('uptime');
      expect(result).toHaveProperty('timestamp');
    });
  });
});
