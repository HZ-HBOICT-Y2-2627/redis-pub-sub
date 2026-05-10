import Redis from 'ioredis';

/**
 * Create an ioredis client from REDIS_URL.
 * Pass blocking: true for connections that will use XREAD BLOCK / XREADGROUP BLOCK,
 * so ioredis doesn't treat a long-running blocked command as a failure.
 */
export function createRedisClient(options: { blocking?: boolean } = {}): Redis {
  const url = process.env.REDIS_URL || 'redis://localhost:6379';
  return new Redis(url, {
    maxRetriesPerRequest: options.blocking ? null : 3,
  });
}
