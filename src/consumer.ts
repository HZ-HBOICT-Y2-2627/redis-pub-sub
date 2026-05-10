import type Redis from 'ioredis';
import { parseFields } from './events';

type MessageHandler = (id: string, data: Record<string, string>) => Promise<void>;

/**
 * Ensure a consumer group exists on the given stream, creating the stream if
 * it does not exist yet (MKSTREAM). Safe to call on every startup.
 */
export async function ensureConsumerGroup(
  redis: Redis,
  stream: string,
  group: string,
): Promise<void> {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err: any) {
    // BUSYGROUP is returned when the group already exists — that is expected.
    if (!err.message?.includes('BUSYGROUP')) throw err;
  }
}

/**
 * Start a background consumer loop using XREADGROUP.
 *
 * - Messages are acknowledged (XACK) only after the handler resolves.
 * - If the handler throws, the message stays in the Pending-Entry List (PEL)
 *   and can be reclaimed later with XAUTOCLAIM.
 * - The loop exits cleanly when the Redis connection is closed.
 */
export function startConsumerLoop(
  redis: Redis,
  stream: string,
  group: string,
  consumer: string,
  handler: MessageHandler,
): void {
  void ensureConsumerGroup(redis, stream, group).then(() => {
    (async () => {
      while (true) {
        try {
          // Cast required: ioredis variadic overloads for XREADGROUP are complex
          const results = await (redis as any).xreadgroup(
            'GROUP', group, consumer,
            'BLOCK', '5000',
            'COUNT', '10',
            'STREAMS', stream, '>',
          ) as Array<[string, Array<[string, string[]]>]> | null;

          if (!results) continue; // BLOCK timeout — loop again

          for (const [, messages] of results) {
            for (const [id, fields] of messages) {
              try {
                await handler(id, parseFields(fields));
                await redis.xack(stream, group, id);
              } catch (err) {
                console.error(`[consumer] Handler failed for ${stream}/${id}:`, err);
                // Left in PEL; reclaim with XAUTOCLAIM after a visibility timeout
              }
            }
          }
        } catch (err: any) {
          if (err.message?.includes('Connection is closed')) break;
          console.error('[consumer] Loop error:', err);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    })();
  });
}
