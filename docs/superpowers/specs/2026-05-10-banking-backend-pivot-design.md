# Banking Backend Pivot — Design

**Date:** 2026-05-10
**Status:** Approved by user, pending implementation plan
**Supersedes:** the e-commerce food-ordering charter in `CLAUDE.md`, `docs/architecture.md`, `docs/operations.md`

## 1. Purpose

Pivot the existing four-service learning project from an e-commerce food-ordering platform to a **Revolut-style banking backend** centered on cross-currency peer-to-peer transfers. The four distributed-systems patterns the project exists to demonstrate are preserved unchanged:

- Microservices with database-per-service.
- Outbox pattern for reliable event publishing.
- Saga orchestration with compensating transactions.
- Idempotent consumers for Kafka redelivery.

Banking is a strong fit for these patterns — money movement is a textbook saga, double-debits are precisely why idempotent consumers exist, and a balance change that publishes an event is the canonical outbox example.

The pivot is **rename in place on `main`**. The Kafka/Postgres/Toxiproxy/Flyway scaffolding, outbox poller, idempotency table, and per-service module shape are all reused. Only the domain layer (entities, status enums, controllers, saga states) changes.

## 2. Core flow

A single saga: **cross-currency P2P transfer.** Alice sends EUR from her wallet, the system FX-converts to USD, and Bob receives USD in his wallet. Both parties get a notification.

```
debit sender wallet  →  FX-convert  →  credit recipient wallet  →  notify
```

Every leg is reversible, which gives a rich compensation chain for studying saga rollbacks.

## 3. Services

The four existing services are renamed and repurposed 1:1. Ports are preserved.

| Service | Replaces | Port | Responsibility |
|---|---|---|---|
| `transfer-service` | `order-service` | 8080 | Saga orchestrator. Owns the transfer lifecycle. Exposes `POST /transfers`. |
| `account-service` | `payment-service` | 8081 | Holds wallets keyed by `(user_id, currency)`. Debits, credits, refunds with optimistic concurrency. |
| `fx-service` | `inventory-service` | 8082 | Looks up rate from a seeded table, performs conversion, records FX trades. |
| `notification-service` | `delivery-service` | 8083 | Records sent notifications. No real push delivery. |

`transfer-service` is the **orchestrator**. The other three are saga participants and emit reply events.

## 4. Saga states

`transfer-service` owns the saga. State machine (mirrors the existing `*_REQUESTED → *_DONE` waiting-state convention from `architecture.md`):

```
PENDING               → DebitAccount sent
DEBIT_REQUESTED       → waiting for AccountDebited/DebitRejected
SENDER_DEBITED        → ConvertCurrency sent
FX_REQUESTED          → waiting for CurrencyConverted/ConversionRejected
FX_CONVERTED          → CreditAccount sent
CREDIT_REQUESTED      → waiting for AccountCredited/CreditRejected
RECIPIENT_CREDITED    → SendNotification sent
NOTIFICATION_REQUESTED → waiting for NotificationSent
COMPLETED             → terminal success
COMPENSATING          → mid-rollback (sub-states optional)
FAILED                → terminal failure
```

Failure paths:

| Failure point | Compensation chain | Terminal state |
|---|---|---|
| `DebitRejected` (insufficient funds, wallet missing) | none — nothing to undo | `FAILED` |
| `ConversionRejected` | `RefundAccount` (re-credit sender) | `FAILED` |
| `CreditRejected` (recipient wallet missing/locked) | `ReverseConversion` → `RefundAccount` | `FAILED` |
| Notification delivery error | none — best-effort, post-success only | `COMPLETED` (notification non-blocking) |

Notification is best-effort by design: a notification failure must not unwind a successful money movement. The saga proceeds to `COMPLETED` once the credit settles, even if the notification leg later fails.

## 5. Data model

All services keep the standard `outbox` and `processed_event` tables (unchanged from current scaffolding).

### transfer-service

```
transfer(
  id UUID PK,
  sender_user TEXT,
  sender_currency TEXT,
  recipient_user TEXT,
  recipient_currency TEXT,
  source_amount NUMERIC(19,4),
  target_amount NUMERIC(19,4),    -- filled after FX
  rate NUMERIC(19,8),               -- filled after FX
  state TEXT,                       -- saga state enum
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
)
```

### account-service

```
wallet(
  id UUID PK,
  user_id TEXT,
  currency TEXT,
  balance NUMERIC(19,4),
  version BIGINT,                   -- optimistic locking
  UNIQUE (user_id, currency)
)

wallet_movement(
  id UUID PK,
  wallet_id UUID FK,
  amount NUMERIC(19,4),             -- always positive; direction is in `type`
  type TEXT,                        -- DEBIT | CREDIT | REFUND
  correlation_id UUID,              -- transfer id from saga
  created_at TIMESTAMPTZ
)
```

Flyway seeds: alice/bob/carol with multi-currency wallets (EUR, USD, GBP) and starting balances.

### fx-service

```
fx_rate(
  base_currency TEXT,
  quote_currency TEXT,
  rate NUMERIC(19,8),
  PRIMARY KEY (base_currency, quote_currency)
)

fx_trade(
  id UUID PK,
  base_currency TEXT,
  quote_currency TEXT,
  base_amount NUMERIC(19,4),
  quote_amount NUMERIC(19,4),
  rate NUMERIC(19,8),
  status TEXT,                      -- EXECUTED | REVERSED
  correlation_id UUID,              -- transfer id
  created_at TIMESTAMPTZ
)
```

Flyway seeds: a small fixed cross-rate matrix (EUR↔USD, EUR↔GBP, USD↔GBP).

### notification-service

```
notification(
  id UUID PK,
  user_id TEXT,
  kind TEXT,                        -- TRANSFER_SENT | TRANSFER_RECEIVED | TRANSFER_FAILED
  payload_json JSONB,
  correlation_id UUID,              -- transfer id
  created_at TIMESTAMPTZ
)
```

## 6. Kafka topics

Per-service `commands` / `events` convention preserved from the existing project.

```
transfer.commands       transfer-service publishes saga commands
transfer.events         transfer state transitions (informational)
account.commands        debit / credit / refund
account.events          debited / credited / debit-rejected / credit-rejected / refunded
fx.commands             convert / reverse
fx.events               converted / rejected / reversed
notification.commands   send notification
notification.events     sent
```

Selected event types (envelope unchanged from current `EventEnvelope.java`):

- `TransferRequested`, `TransferCompleted`, `TransferFailed`
- `DebitAccount`, `CreditAccount`, `RefundAccount`
- `AccountDebited`, `AccountCredited`, `DebitRejected`, `CreditRejected`, `DebitRefunded`
- `ConvertCurrency`, `ReverseConversion`
- `CurrencyConverted`, `ConversionRejected`, `FxReversed`
- `SendNotification`, `NotificationSent`

## 7. REST surface

| Method | Path | Service | Purpose |
|---|---|---|---|
| POST | `/transfers` | transfer | Initiate. Body: `{senderUser, recipientUser, sourceCurrency, sourceAmount, targetCurrency}`. Returns `transferId`. |
| GET | `/transfers/{id}` | transfer | Saga state for inspection. |
| GET | `/wallets/{user}/{currency}` | account | Balance lookup (debug). |
| GET | `/fx/rates` | fx | List seeded rates. |
| GET | `/notifications/{user}` | notification | Recent notifications (debug). |

## 8. Conventions preserved from current scaffolding

- **Event envelope** (current `EventEnvelope.java`): `eventId`, `eventType`, `correlationId`, `occurredAt`, `payload`. Unchanged.
- **Idempotency**: each consumer checks `processed_event(event_id)` before applying a side effect; inserts the row in the same DB transaction as the side effect.
- **Outbox poller**: each service's `OutboxPublisher` continues to poll its `outbox` table and publish to Kafka. Unchanged in shape.
- **Per-service Postgres**: still one Postgres instance per service; database names rename from `orderdb`/`paymentdb`/`inventorydb`/`deliverydb` to `transferdb`/`accountdb`/`fxdb`/`notificationdb`.
- **Toxiproxy chaos**: still in front of Kafka, still driven by `tools/chaos.sh`. Toxic fault injection (latency, jitter, partitions) is the source of redelivery and timeout scenarios — FX-service does not synthesize failures itself.
- **kafka-ui**: unchanged.
- **No host-side `psql` / `kcat`**: inspect via `docker exec` only.

## 9. FX scope

Stubbed table of fixed rates seeded via Flyway. No external API. Rates are read at conversion time (no quote-and-hold TTL). The FX-service compensation step (`ReverseConversion`) writes a paired `REVERSED` row in `fx_trade` referencing the original trade by `correlation_id`.

## 10. Rename mapping (the actual pivot work)

### Module directories

```
order-service       → transfer-service
payment-service     → account-service
inventory-service   → fx-service
delivery-service    → notification-service
```

### Java packages

```
com.outboxsagalab.order        → com.outboxsagalab.transfer
com.outboxsagalab.payment      → com.outboxsagalab.account
com.outboxsagalab.inventory    → com.outboxsagalab.fx
com.outboxsagalab.delivery     → com.outboxsagalab.notification
```

### Domain classes (selected)

| Old | New |
|---|---|
| `Order`, `OrderStatus`, `OrderSaga` | `Transfer`, `TransferState`, `TransferSaga` |
| `Payment`, `PaymentStatus`, `PaymentService` | `Wallet`, `WalletMovement`, `WalletMovementType`, `AccountService` |
| `Reservation`, `ReservationItem`, `Stock`, `InventoryService` | `FxTrade`, `FxRate`, `FxTradeStatus`, `FxService` |
| `Delivery`, `DeliveryStatus`, `DeliveryService` | `Notification`, `NotificationKind`, `NotificationService` |
| `OrderDebugController` / `StockController` / `DeliveryDebugController` / `PaymentDebugController` | `TransferController`, `WalletController`, `FxController`, `NotificationController` |

### Database names (docker-compose.yml + each `application.yml`)

```
orderdb       → transferdb
paymentdb     → accountdb
inventorydb   → fxdb
deliverydb    → notificationdb
```

### Flyway

The existing `V1__init.sql` files in each service describe the e-commerce schema and have never been applied to a real database (project has not run end-to-end). Drop them and write fresh `V1__init.sql` files for the banking schema. Add `V2__seed_*.sql` in `account-service` (wallets) and `fx-service` (rates).

### Topics (`Topics.java` + listener subscriptions)

Update each service's constants and `@KafkaListener` topics to the new names listed in §6.

### Docs

- `CLAUDE.md` — rewrite the charter to describe the banking domain. Keep the Working Agreements section verbatim.
- `docs/architecture.md` — rewrite mermaid diagrams (saga state, outbox flow, idempotency check, compensation routing), conventions table, chaos topology section. Banking-domain examples throughout.
- `docs/operations.md` — rewrite `docker exec` recipes (`transferdb` / `accountdb` / etc.), traffic-sim payloads (transfer requests instead of orders), chaos commands (still toxiproxy-driven).

### Traffic sim

Rewrite `traffic-sim/scenarios.js` to POST cross-currency transfers across the seeded users, with a mix of currencies that hit different `fx_rate` rows.

## 11. Out of scope

Preserved from current charter, with banking-flavored additions:

- No real payment networks (SEPA, SWIFT, card networks).
- No real auth / users — seed alice/bob/carol via Flyway.
- No real notification delivery — record intent only.
- No FX rate streaming, no quote-and-hold TTL — convert at lookup time.
- No fraud / AML / KYC engines.
- Frontend, multi-broker Kafka, schema registry, Avro/Protobuf — still out.
- Production concerns (Kubernetes, service mesh, distributed tracing, complex retry libraries) — still out.
