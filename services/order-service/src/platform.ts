/**
 * Shared plumbing (config, logging, Postgres, Kafka, outbox relay).
 * Each service keeps its own copy so services can be built and deployed independently.
 */
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type Producer } from 'kafkajs';
import pg from 'pg';
import { pino } from 'pino';

/** 12-factor config: everything comes from the environment. */
export function env(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (value === undefined) throw new Error(`Missing environment variable ${name}`);
  return value;
}

export const SERVICE = env('SERVICE_NAME');
export const log = pino({ base: { service: SERVICE }, level: env('LOG_LEVEL', 'info') });

export const db = new pg.Pool({ connectionString: env('DATABASE_URL'), max: 10 });

export const kafka = new Kafka({
  clientId: SERVICE,
  brokers: env('KAFKA_BROKERS').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 10 },
});

/** Every event shares this envelope. `id` lets consumers de-duplicate redeliveries. */
export interface EventEnvelope<T = Record<string, unknown>> {
  id: string;
  type: string;
  occurredAt: string;
  source: string;
  data: T;
}

export function newEvent<T>(type: string, data: T): EventEnvelope<T> {
  return { id: randomUUID(), type, occurredAt: new Date().toISOString(), source: SERVICE, data };
}

export async function ensureTopics(topics: string[]) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: topics.map((topic) => ({ topic, numPartitions: 3, replicationFactor: 1 })),
    });
  } catch (error) {
    // Several services start together; another one may have created the topic first.
    if (!String(error).includes('already exists')) throw error;
  } finally {
    await admin.disconnect();
  }
}

/** Outbox table used by the relay below. Created by every service that publishes events. */
export const OUTBOX_DDL = `
  CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    message_key  TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
  );
  CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;`;

/** Write an event to the outbox inside the caller's database transaction. */
export async function enqueue(client: pg.PoolClient, topic: string, key: string, event: EventEnvelope) {
  await client.query('INSERT INTO outbox (topic, message_key, payload) VALUES ($1, $2, $3)', [topic, key, event]);
}

/**
 * Transactional outbox relay: publish committed outbox rows to Kafka, then mark them sent.
 * If the service crashes between send and update, the row is sent again (at-least-once),
 * which is why every consumer de-duplicates on the event id.
 * FOR UPDATE SKIP LOCKED lets several replicas run the relay without sending a row twice at once.
 */
export function startOutboxRelay(producer: Producer, intervalMs = 300) {
  let stopped = false;
  const tick = async () => {
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id, topic, message_key, payload FROM outbox
         WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED`,
      );
      for (const row of rows) {
        await producer.send({
          topic: row.topic,
          messages: [{ key: row.message_key, value: JSON.stringify(row.payload), headers: { 'event-type': row.payload.type } }],
        });
      }
      if (rows.length) {
        await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1)', [rows.map((r) => r.id)]);
        log.debug({ count: rows.length }, 'outbox published');
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      log.error({ err: error }, 'outbox relay failed; will retry');
    } finally {
      client.release();
      if (!stopped) setTimeout(tick, intervalMs);
    }
  };
  setTimeout(tick, intervalMs);
  return () => { stopped = true; };
}

/**
 * Run `handler` at most once per event id: the id is recorded in the same database
 * transaction as the handler's writes, so a redelivered event is skipped.
 */
export async function handleOnce(eventId: string, handler: (client: pg.PoolClient) => Promise<void>) {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const fresh = await client.query(
      'INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING',
      [eventId],
    );
    if (fresh.rowCount === 0) {
      await client.query('ROLLBACK');
      log.info({ eventId }, 'duplicate event skipped');
      return false;
    }
    await handler(client);
    await client.query('COMMIT');
    return true;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

export const PROCESSED_DDL = `
  CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );`;

/** Graceful shutdown so in-flight work finishes and consumers leave their group cleanly. */
export function onShutdown(cleanup: () => Promise<void>) {
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, async () => {
      log.info({ signal }, 'shutting down');
      await cleanup().catch((err) => log.error({ err }, 'shutdown error'));
      await db.end();
      process.exit(0);
    });
  }
}
