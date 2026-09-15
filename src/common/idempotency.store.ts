import { createHash } from 'node:crypto';

export class IdempotencyStore {
  // Save hash -> expiration timestamp (TTL)
  private readonly store = new Map<string, number>();

  constructor(private readonly defaultTtlMs = 120_000) {} // 2 minutes by default

  /**
   * Create a deterministic SHA-256 hash from any object
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
   * Check if an event with this hash has already been processed
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
   * Fixes the processing of an event in the store with TTL
   */
  public set(key: string, ttlMs: number = this.defaultTtlMs): void {
    this.store.set(key, Date.now() + ttlMs);
  }
}
