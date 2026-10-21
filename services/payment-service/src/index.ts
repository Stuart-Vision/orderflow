/**
 * Payment service: consumes `order.created`, charges the order (simulated), and publishes
 * `payment.completed` or `payment.failed`. Orders above the limit are declined so both
 * paths can be seen end to end.
 */
import express from 'express';
import {
  OUTBOX_DDL, PROCESSED_DDL, db, enqueue, ensureTopics, env, handleOnce, kafka, log, newEvent, onShutdown,
  startOutboxRelay, type EventEnvelope,
} from './platform.js';

const PORT = Number(env('PORT', '3002'));
const LIMIT_CENTS = Number(env('PAYMENT_LIMIT_CENTS', '100000')); // 1,000.00

await db.query(`
  CREATE TABLE IF NOT EXISTS payments (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id     UUID NOT NULL UNIQUE,
    amount_cents INTEGER NOT NULL,
    status       TEXT NOT NULL,
    reason       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ${OUTBOX_DDL}
  ${PROCESSED_DDL}`);

await ensureTopics(['orders', 'payments']);
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
await producer.connect();
const stopRelay = startOutboxRelay(producer);

type OrderCreated = { orderId: string; customerId: string; totalCents: number };

const consumer = kafka.consumer({ groupId: 'payment-service' });
await consumer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: true });
await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value!.toString()) as EventEnvelope<OrderCreated>;
    if (event.type !== 'order.created') return;
    const { orderId, totalCents } = event.data;
    const approved = totalCents <= LIMIT_CENTS;

    // The payment row, its outgoing event and the processed-event marker commit together,
    // so a redelivered `order.created` can never charge the same order twice.
    const handled = await handleOnce(event.id, async (client) => {
      await client.query(
        `INSERT INTO payments (order_id, amount_cents, status, reason) VALUES ($1, $2, $3, $4)`,
        [orderId, totalCents, approved ? 'COMPLETED' : 'FAILED', approved ? null : 'amount_over_limit'],
      );
      await enqueue(client, 'payments', orderId, newEvent(approved ? 'payment.completed' : 'payment.failed', {
        orderId, amountCents: totalCents, ...(approved ? {} : { reason: 'amount_over_limit' }),
      }));
    });
    if (handled) log.info({ orderId, approved }, 'payment processed');
  },
});

const app = express();

app.get('/health', async (_req, res) => {
  await db.query('SELECT 1');
  res.json({ status: 'ok', service: 'payment-service' });
});

app.get('/payments', async (req, res) => {
  const orderId = typeof req.query.orderId === 'string' ? req.query.orderId : null;
  const { rows } = orderId
    ? await db.query('SELECT * FROM payments WHERE order_id = $1', [orderId]).catch(() => ({ rows: [] }))
    : await db.query('SELECT * FROM payments ORDER BY created_at DESC LIMIT 100');
  res.json(rows);
});

const server = app.listen(PORT, () => log.info({ port: PORT }, 'payment-service listening'));

onShutdown(async () => {
  stopRelay();
  server.close();
  await consumer.disconnect();
  await producer.disconnect();
});
