/**
 * Order service: accepts orders over REST, publishes `order.created`, and moves each
 * order to PAID or PAYMENT_FAILED when the payment service reports back.
 */
import express from 'express';
import {
  OUTBOX_DDL, PROCESSED_DDL, db, enqueue, ensureTopics, env, handleOnce, kafka, log, newEvent, onShutdown,
  startOutboxRelay, type EventEnvelope,
} from './platform.js';

const PORT = Number(env('PORT', '3001'));

await db.query(`
  CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id TEXT NOT NULL,
    items       JSONB NOT NULL,
    total_cents INTEGER NOT NULL CHECK (total_cents > 0),
    status      TEXT NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ${OUTBOX_DDL}
  ${PROCESSED_DDL}`);

await ensureTopics(['orders', 'payments']);
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
await producer.connect();
const stopRelay = startOutboxRelay(producer);

// React to payment results.
const consumer = kafka.consumer({ groupId: 'order-service' });
await consumer.connect();
await consumer.subscribe({ topic: 'payments', fromBeginning: true });
await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value!.toString()) as EventEnvelope<{ orderId: string }>;
    const status = event.type === 'payment.completed' ? 'PAID' : event.type === 'payment.failed' ? 'PAYMENT_FAILED' : null;
    if (!status) return;
    await handleOnce(event.id, async (client) => {
      await client.query(
        `UPDATE orders SET status = $2, updated_at = now() WHERE id = $1 AND status = 'PENDING'`,
        [event.data.orderId, status],
      );
    });
    log.info({ orderId: event.data.orderId, status }, 'order status updated');
  },
});

const app = express();
app.use(express.json({ limit: '100kb' }));

app.get('/health', async (_req, res) => {
  await db.query('SELECT 1');
  res.json({ status: 'ok', service: 'order-service' });
});

type Item = { sku: string; quantity: number; unitPriceCents: number };

app.post('/orders', async (req, res) => {
  const { customerId, items } = req.body ?? {};
  const valid = typeof customerId === 'string' && customerId.length > 0 && Array.isArray(items) && items.length > 0
    && items.every((i: Item) => typeof i?.sku === 'string' && Number.isInteger(i.quantity) && i.quantity > 0
      && Number.isInteger(i.unitPriceCents) && i.unitPriceCents > 0);
  if (!valid) {
    res.status(400).json({ error: 'Expected { customerId, items: [{ sku, quantity, unitPriceCents }] }' });
    return;
  }
  const total = (items as Item[]).reduce((sum, i) => sum + i.quantity * i.unitPriceCents, 0);

  // The order row and its event are committed together (transactional outbox),
  // so an event is published if and only if the order exists.
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      'INSERT INTO orders (customer_id, items, total_cents) VALUES ($1, $2, $3) RETURNING *',
      [customerId, JSON.stringify(items), total],
    );
    const order = rows[0];
    await enqueue(client, 'orders', order.id, newEvent('order.created', {
      orderId: order.id, customerId, totalCents: total,
    }));
    await client.query('COMMIT');
    log.info({ orderId: order.id, totalCents: total }, 'order created');
    res.status(201).json(order);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.error({ err: error }, 'create order failed');
    res.status(500).json({ error: 'Could not create order' });
  } finally {
    client.release();
  }
});

app.get('/orders', async (_req, res) => {
  const { rows } = await db.query('SELECT * FROM orders ORDER BY created_at DESC LIMIT 100');
  res.json(rows);
});

app.get('/orders/:id', async (req, res) => {
  const { rows } = await db.query('SELECT * FROM orders WHERE id = $1', [req.params.id]).catch(() => ({ rows: [] }));
  if (!rows[0]) {
    res.status(404).json({ error: 'Order not found' });
    return;
  }
  res.json(rows[0]);
});

/**
 * Test hook: mark this order's events as unpublished so the relay sends them again,
 * simulating Kafka's at-least-once redelivery. Disabled unless ENABLE_TEST_HOOKS=true.
 */
app.post('/orders/:id/redeliver', async (req, res) => {
  if (env('ENABLE_TEST_HOOKS', 'false') !== 'true') {
    res.status(404).end();
    return;
  }
  const { rowCount } = await db.query('UPDATE outbox SET published_at = NULL WHERE message_key = $1', [req.params.id]);
  res.json({ requeued: rowCount });
});

const server = app.listen(PORT, () => log.info({ port: PORT }, 'order-service listening'));

onShutdown(async () => {
  stopRelay();
  server.close();
  await consumer.disconnect();
  await producer.disconnect();
});
