import type Redis from 'ioredis';
import { flattenEvent } from './events';

/**
 * Append an event to a Redis Stream using XADD with auto-generated ID.
 * Returns the message ID assigned by Redis.
 */
export async function publish(
  redis: Redis,
  stream: string,
  event: Record<string, string>,
): Promise<string> {
  const fields = flattenEvent(event);
  return redis.xadd(stream, '*', ...fields) as Promise<string>;
}
