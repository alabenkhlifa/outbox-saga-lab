import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    happy_path: {
      executor: 'constant-arrival-rate',
      rate: 5,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 10,
      maxVUs: 20,
      exec: 'happyPath',
    },
  },
};

const TRANSFER_URL = 'http://localhost:8080/transfers';

const USERS = ['alice', 'bob', 'carol'];

// Pairs that map to seeded wallets AND seeded fx_rate rows.
const TRANSFER_PAIRS = [
  { senderCurrency: 'EUR', recipientCurrency: 'USD' },
  { senderCurrency: 'USD', recipientCurrency: 'EUR' },
  { senderCurrency: 'EUR', recipientCurrency: 'GBP' },
  { senderCurrency: 'GBP', recipientCurrency: 'EUR' },
  { senderCurrency: 'USD', recipientCurrency: 'GBP' },
];

export function happyPath() {
  const sender = randomItem(USERS);
  let recipient = randomItem(USERS);
  while (recipient === sender) recipient = randomItem(USERS);

  const pair = randomItem(TRANSFER_PAIRS);
  const amount = (Math.floor(Math.random() * 50) + 1).toFixed(2); // 1-50, two decimals

  const payload = JSON.stringify({
    senderUser: sender,
    senderCurrency: pair.senderCurrency,
    recipientUser: recipient,
    recipientCurrency: pair.recipientCurrency,
    sourceAmount: amount,
  });

  const res = http.post(TRANSFER_URL, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 201': (r) => r.status === 201,
  });

  sleep(0.2);
}
