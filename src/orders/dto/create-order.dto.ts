import { IsEmail, IsNotEmpty, IsNumber, Min } from 'class-validator';

export class CreateOrderDto {
  @IsEmail()
  @IsNotEmpty()
  customerEmail: string;

  @IsNumber()
  @Min(0.01)
  totalPrice: number;
}
