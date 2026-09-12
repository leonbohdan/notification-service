import { IsEmail, IsNotEmpty } from 'class-validator';

export class CreateNotificationDto {
  @IsNotEmpty()
  orderId: string;

  @IsEmail()
  customerEmail: string;

  @IsNotEmpty()
  message: string;
}