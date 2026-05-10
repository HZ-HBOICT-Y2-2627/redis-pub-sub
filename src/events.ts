// ─── Stream names ─────────────────────────────────────────────────────────────
export const STREAM_CARS     = 'events:cars';
export const STREAM_AUCTIONS = 'events:auctions';

// Reply streams are ephemeral, keyed by correlationId: reply:<uuid>
export const REPLY_PREFIX = 'reply';

// ─── Consumer group names ─────────────────────────────────────────────────────
export const GROUP_AUCTION_SERVICE = 'auction-service';
export const GROUP_CARS_CATALOG    = 'cars-catalog-service';

// ─── Event shapes ─────────────────────────────────────────────────────────────
// All field values are strings — Redis Streams store everything as strings.
// Callers are responsible for parsing numbers / dates after reading.

export interface CarDeletionRequestedEvent {
  type: 'car.deletion.requested';
  carId: string;         // number serialised as string
  correlationId: string; // UUID — links request to its saga reply
  timestamp: string;     // ISO-8601
}

export interface AuctionsCancelledForCarEvent {
  type: 'auctions.cancelled_for_car';
  carId: string;
  correlationId: string;
  cancelledCount: string;
}

export interface AuctionCancellationFailedEvent {
  type: 'auctions.cancellation_failed';
  carId: string;
  correlationId: string;
  reason: string;
}

export type CarEvent         = CarDeletionRequestedEvent;
export type AuctionReplyEvent = AuctionsCancelledForCarEvent | AuctionCancellationFailedEvent;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Convert a flat Redis field array [k, v, k, v, …] into a plain object. */
export function parseFields(fields: string[]): Record<string, string> {
  const result: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    result[fields[i]] = fields[i + 1];
  }
  return result;
}

/** Flatten a plain object into a Redis field array [k, v, k, v, …]. */
export function flattenEvent(event: Record<string, string>): string[] {
  return Object.entries(event).flat();
}
