import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from 'typeorm';

export enum OutboxStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  PUBLISHED = 'PUBLISHED',
  FAILED = 'FAILED',
}

@Entity('outbox_events')
export class OutboxEvent {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  aggregateType: string; // Наприклад: 'Order'

  @Column()
  aggregateId: string; // ID сутності, якої стосується подія

  @Column()
  eventType: string; // Наприклад: 'ORDER_CREATED'

  @Column('jsonb')
  payload: Record<string, any>; // Тіло події у форматі JSON

  @Index() // Індекс необхідний для швидкої вибірки воркером у Завданні 2!
  @Column({
    type: 'enum',
    enum: OutboxStatus,
    default: OutboxStatus.PENDING,
  })
  status: OutboxStatus;

  @Column({ default: 0 })
  retryCount: number;

  @Column({ nullable: true })
  errorMessage: string;

  @CreateDateColumn()
  createdAt: Date;

  @Column({ nullable: true })
  processedAt: Date;
}
