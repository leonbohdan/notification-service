import { createHash } from 'node:crypto';

export class IdempotencyStore {
  // Зберігаємо hash -> timestamp закінчення терміну дії (TTL)
  private readonly store = new Map<string, number>();

  constructor(private readonly defaultTtlMs = 120_000) {} // 2 хвилини за замовчуванням

  /**
   * Створює детермінований SHA-256 хеш з будь-якого об'єкта
   */
  public generateHash(payload: unknown): string {
    const sortedString = JSON.stringify(payload, (_, val) =>
      val && typeof val === 'object' && !Array.isArray(val)
        ? Object.keys(val)
            .sort()
            .reduce((acc: Record<string, any>, key) => {
              acc[key] = val[key];
              return acc;
            }, {})
        : val,
    );

    return createHash('sha256').update(sortedString).digest('hex');
  }

  /**
   * Перевіряє, чи подія з таким хешем уже оброблялася
   */
  public has(key: string): boolean {
    const expiry = this.store.get(key);
    if (!expiry) return false;

    if (Date.now() > expiry) {
      this.store.delete(key);
      return false;
    }
    return true;
  }

  /**
   * Фіксує обробку події у сховищі з TTL
   */
  public set(key: string, ttlMs: number = this.defaultTtlMs): void {
    this.store.set(key, Date.now() + ttlMs);
  }
}
