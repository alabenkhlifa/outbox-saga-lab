// Traffic simulator for outbox-saga-lab.
//
// Three concurrent scenarios run in one k6 process. Each models a different
// kind of traffic that is interesting for the saga / outbox / idempotency
// architecture — together they cover steady load, sudden bursts, and a
// hot-SKU contention pattern that forces compensation.
//
// Run all scenarios:
//   k6 run traffic-sim/scenarios.js
//
// Run a single scenario:
//   k6 run --env SCENARIO=diurnal  traffic-sim/scenarios.js
//   k6 run --env SCENARIO=burst    traffic-sim/scenarios.js
//   k6 run --env SCENARIO=hotSku   traffic-sim/scenarios.js
//
// Override the order-service URL:
//   k6 run --env BASE_URL=http://localhost:8080 traffic-sim/scenarios.js
//
// Required: order-service reachable at BASE_URL, plus the rest of the stack
// (Kafka, the four databases) up so the saga can play out end-to-end.

import http from 'k6/http';
import { check } from 'k6';
import { uuidv4, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const SCENARIO = __ENV.SCENARIO; // optional filter — see selectScenarios() below

// SKU catalog — must match V2__seed_stock.sql in inventory-service.
// `weight` is a relative probability used by the order builder.
const MENU = [
  { sku: 'pizza-margherita', price: 12.50, weight: 40 },
  { sku: 'burger-deluxe',    price: 14.00, weight: 30 },
  { sku: 'salad-caesar',     price:  9.50, weight: 20 },
  { sku: 'soda-cola',        price:  3.00, weight: 10 }, // intentionally low stock (qty 5)
];

const TOTAL_WEIGHT = MENU.reduce((acc, m) => acc + m.weight, 0);
const CUSTOMER_POOL_SIZE = 50; // a small pool that "comes back" — repeat customers feel real

function pickItem() {
  let r = Math.random() * TOTAL_WEIGHT;
  for (const item of MENU) {
    r -= item.weight;
    if (r <= 0) return item;
  }
  return MENU[MENU.length - 1];
}

function pickCustomerId() {
  return `cust-${randomIntBetween(1, CUSTOMER_POOL_SIZE)}`;
}

// Realistic order-size distribution: mostly small, occasional large group order.
function pickItemCount() {
  const r = Math.random();
  if (r < 0.80) return randomIntBetween(1, 2);
  if (r < 0.95) return randomIntBetween(3, 5);
  return randomIntBetween(6, 12);
}

function buildOrder(items) {
  return JSON.stringify({
    customerId: pickCustomerId(),
    items: items.map((i) => ({
      sku: i.sku,
      quantity: i.quantity,
      unitPrice: i.price,
    })),
  });
}

function postOrder(payload, tag) {
  const res = http.post(`${BASE_URL}/orders`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': uuidv4(), // header is informational; service uses its own UUID
    },
    tags: { scenario: tag },
  });

  check(res, {
    'status is 201': (r) => r.status === 201,
    'returns Location': (r) => !!r.headers['Location'],
  });
}

// -------------------------------------------------------------------------
// Scenario exec functions
// -------------------------------------------------------------------------

// Default order pattern: random size, weighted SKU mix.
export function normalOrder() {
  const count = pickItemCount();
  const items = [];
  for (let i = 0; i < count; i++) {
    const m = pickItem();
    items.push({ sku: m.sku, price: m.price, quantity: randomIntBetween(1, 3) });
  }
  postOrder(buildOrder(items), 'normal');
}

// Hot-SKU pattern: every order asks for soda-cola (low stock), forcing
// concurrent reservation contention on inventory-service.
export function sodaColaOrder() {
  const items = [
    { sku: 'soda-cola', price: 3.00, quantity: randomIntBetween(1, 3) },
  ];
  if (Math.random() < 0.5) {
    // sometimes pair the soda with a regular item — more realistic
    const m = pickItem();
    if (m.sku !== 'soda-cola') {
      items.push({ sku: m.sku, price: m.price, quantity: 1 });
    }
  }
  postOrder(buildOrder(items), 'hotSku');
}

// -------------------------------------------------------------------------
// Scenario definitions
// -------------------------------------------------------------------------

const ALL_SCENARIOS = {
  // Compressed-day diurnal curve. Ramping arrival rate hits two peaks
  // (lunch ~ 12, dinner ~ 19) and a quiet middle. Useful for watching the
  // outbox poller catch up after the surge.
  diurnal: {
    executor: 'ramping-arrival-rate',
    startRate: 5,
    timeUnit: '1s',
    preAllocatedVUs: 50,
    maxVUs: 200,
    stages: [
      { duration: '20s', target: 5 },   // early morning
      { duration: '20s', target: 30 },  // morning ramp
      { duration: '30s', target: 60 },  // lunch peak
      { duration: '30s', target: 15 },  // afternoon dip
      { duration: '30s', target: 70 },  // dinner peak
      { duration: '20s', target: 5 },   // wind down
    ],
    exec: 'normalOrder',
  },

  // Sudden spike from steady-state. Watch for Kafka consumer lag and outbox
  // backlog growth, then drain time once the spike is over.
  burst: {
    executor: 'ramping-arrival-rate',
    startRate: 5,
    timeUnit: '1s',
    preAllocatedVUs: 100,
    maxVUs: 300,
    stages: [
      { duration: '30s', target: 5 },   // baseline
      { duration: '5s',  target: 100 }, // sudden spike
      { duration: '30s', target: 100 }, // sustained
      { duration: '5s',  target: 5 },   // back down
      { duration: '30s', target: 5 },   // recovery
    ],
    exec: 'normalOrder',
  },

  // Hot-SKU contention. With only 5 units of soda-cola seeded, most of these
  // orders should fail at inventory and trigger payment refund + FAILED.
  hotSku: {
    executor: 'constant-arrival-rate',
    rate: 8,
    timeUnit: '1s',
    duration: '1m',
    preAllocatedVUs: 30,
    maxVUs: 80,
    exec: 'sodaColaOrder',
  },
};

function selectScenarios() {
  if (!SCENARIO) return ALL_SCENARIOS;
  const picked = ALL_SCENARIOS[SCENARIO];
  if (!picked) {
    throw new Error(`unknown SCENARIO=${SCENARIO}; valid: ${Object.keys(ALL_SCENARIOS).join(', ')}`);
  }
  return { [SCENARIO]: picked };
}

export const options = {
  scenarios: selectScenarios(),
  thresholds: {
    // Soft expectations — the saga runs through Kafka so HTTP returning
    // 201 just means the order was accepted, not that the saga completed.
    'http_req_failed{scenario:normal}': ['rate<0.01'],
    'http_req_duration{scenario:normal}': ['p(95)<500'],
    // hotSku scenario is expected to surface 4xx from order-service... no, actually
    // POST always succeeds (saga drives state asynchronously). So we still expect 201.
    'http_req_failed{scenario:hotSku}': ['rate<0.01'],
  },
};
