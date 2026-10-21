/**
 * Notification service: an independent consumer group on both topics. It turns events
 * into customer notifications (stored here; a real system would send email or SMS).
 * Shows fan-out: the same events feed several services without any of them knowing.
 */
import express from 'express';
import { PROCESSED_DDL, db, ensureTopics, env, handleOnce, kafka, log, onShutdown, type EventEnvelope } from './platform.js';

const PORT = Number(env('PORT', '3003'));

await db.query(`
  CREATE TABLE IF NOT EXISTS notifications (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID NOT NULL UNIQUE,
    order_id   UUID NOT NULL,
    message    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ${PROCESSED_DDL}`);

await ensureTopics(['orders', 'payments']);

const messages: Record<string, (d: Record<string, unknown>) => string> = {
  'order.created': (d) => `Order ${d.orderId} received (total ${(Number(d.totalCents) / 100).toFixed(2)}).`,
  'payment.completed': (d) => `Payment for order ${d.orderId} succeeded.`,
  'payment.failed': (d) => `Payment for order ${d.orderId} failed: ${d.reason}.`,
};

const consumer = kafka.consumer({ groupId: 'notification-service' });
await consumer.connect();
await consumer.subscribe({ topics: ['orders', 'payments'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value!.toString()) as EventEnvelope<Record<string, unknown>>;
    const render = messages[event.type];
    if (!render) return;
    await handleOnce(event.id, async (client) => {
      await client.query(
        'INSERT INTO notifications (event_id, order_id, message) VALUES ($1, $2, $3)',
        [event.id, event.data.orderId, render(event.data)],
      );
    });
  },
});

const app = express();

app.get('/health', async (_req, res) => {
  await db.query('SELECT 1');
  res.json({ status: 'ok', service: 'notification-service' });
});

app.get('/notifications', async (req, res) => {
  const orderId = typeof req.query.orderId === 'string' ? req.query.orderId : null;
  const { rows } = orderId
    ? await db.query('SELECT * FROM notifications WHERE order_id = $1 ORDER BY id', [orderId]).catch(() => ({ rows: [] }))
    : await db.query('SELECT * FROM notifications ORDER BY id DESC LIMIT 100');
  res.json(rows);
});

const server = app.listen(PORT, () => log.info({ port: PORT }, 'notification-service listening'));

onShutdown(async () => {
  server.close();
  await consumer.disconnect();
});
