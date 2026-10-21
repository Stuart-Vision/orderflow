/**
 * End-to-end test against the running stack (docker compose up).
 *
 * 1. Creates 6 orders: 4 under the 1,000.00 payment limit, 2 over it.
 * 2. Waits for the event flow order -> payment -> order/notification to settle.
 * 3. Forces a redelivery of one order's event and checks it is not charged twice.
 */
const ORDERS = process.env.ORDERS_URL ?? 'http://localhost:3001';
const PAYMENTS = process.env.PAYMENTS_URL ?? 'http://localhost:3002';
const NOTIFY = process.env.NOTIFY_URL ?? 'http://localhost:3003';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const json = async (url, init) => {
  const res = await fetch(url, init);
  if (!res.ok) throw new Error(`${init?.method ?? 'GET'} ${url} -> ${res.status}`);
  return res.json();
};
async function waitFor(check, label, timeoutMs = 60_000) {
  const start = Date.now();
  for (;;) {
    const result = await check().catch(() => false);
    if (result) return result;
    if (Date.now() - start > timeoutMs) throw new Error(`Timed out waiting for ${label}`);
    await sleep(500);
  }
}

let failures = 0;
const expect = (ok, message) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${message}`);
  if (!ok) failures++;
};

for (const url of [ORDERS, PAYMENTS, NOTIFY]) await waitFor(() => json(`${url}/health`), `${url}/health`, 120_000);

const totals = [4_999, 25_000, 99_999, 100_000, 100_001, 250_000];
const started = Date.now();
const created = await Promise.all(totals.map((cents, i) => json(`${ORDERS}/orders`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ customerId: `cust-${i}`, items: [{ sku: `SKU-${i}`, quantity: 1, unitPriceCents: cents }] }),
})));
expect(created.every((o) => o.status === 'PENDING'), 'orders are created as PENDING');

const settled = await waitFor(async () => {
  const orders = await Promise.all(created.map((o) => json(`${ORDERS}/orders/${o.id}`)));
  return orders.every((o) => o.status !== 'PENDING') && orders;
}, 'orders to settle');
console.log(`      all orders settled in ${Date.now() - started} ms`);

const statusOf = Object.fromEntries(settled.map((o) => [o.total_cents, o.status]));
expect(totals.filter((t) => t <= 100_000).every((t) => statusOf[t] === 'PAID'), 'orders up to 1,000.00 are PAID');
expect(totals.filter((t) => t > 100_000).every((t) => statusOf[t] === 'PAYMENT_FAILED'), 'orders over 1,000.00 are PAYMENT_FAILED');

const notes = await waitFor(async () => {
  const lists = await Promise.all(created.map((o) => json(`${NOTIFY}/notifications?orderId=${o.id}`)));
  return lists.every((l) => l.length === 2) && lists;
}, 'notifications');
expect(notes.every((l) => l.length === 2), 'each order gets exactly 2 notifications (created + payment result)');

// At-least-once delivery: replay one order's event and confirm the consumer de-duplicates it.
const target = created[0].id;
const { requeued } = await json(`${ORDERS}/orders/${target}/redeliver`, { method: 'POST' });
expect(requeued >= 1, 'order event re-queued for redelivery');
await sleep(4000);
const payments = await json(`${PAYMENTS}/payments?orderId=${target}`);
expect(payments.length === 1, `redelivered event did not create a second payment (payments: ${payments.length})`);
const notesAfter = await json(`${NOTIFY}/notifications?orderId=${target}`);
expect(notesAfter.length === 2, `redelivered event did not create a duplicate notification (notifications: ${notesAfter.length})`);

console.log(failures ? `\n${failures} check(s) failed` : '\nAll checks passed');
process.exit(failures ? 1 : 0);
