# Architecture

Living design doc for `outbox-saga-lab`. All four services and the subagents scaffolding them must follow the conventions in this file.

---

## 1. System architecture

```mermaid
flowchart LR
    Client([REST Client])

    subgraph orderSvc["order-service (saga orchestrator)"]
        OrderApp[Spring Boot]
        OrderDB[(orders<br/>:5432)]
        OrderApp --- OrderDB
    end

    subgraph paymentSvc["payment-service"]
        PaymentApp[Spring Boot]
        PaymentDB[(payments<br/>:5433)]
        PaymentApp --- PaymentDB
    end

    subgraph inventorySvc["inventory-service"]
        InventoryApp[Spring Boot]
        InventoryDB[(inventory<br/>:5434)]
        InventoryApp --- InventoryDB
    end

    subgraph deliverySvc["delivery-service"]
        DeliveryApp[Spring Boot]
        DeliveryDB[(delivery<br/>:5435)]
        DeliveryApp --- DeliveryDB
    end

    Kafka{{Kafka<br/>:9092}}

    Client -->|POST /orders| OrderApp

    OrderApp -- "payment-commands" --> Kafka
    OrderApp -- "inventory-commands" --> Kafka
    OrderApp -- "delivery-commands" --> Kafka

    Kafka -- "payment-events" --> OrderApp
    Kafka -- "inventory-events" --> OrderApp
    Kafka -- "delivery-events" --> OrderApp

    Kafka -- "payment-commands" --> PaymentApp
    PaymentApp -- "payment-events" --> Kafka

    Kafka -- "inventory-commands" --> InventoryApp
    InventoryApp -- "inventory-events" --> Kafka

    Kafka -- "delivery-commands" --> DeliveryApp
    DeliveryApp -- "delivery-events" --> Kafka
```

**Topology**: Order Service is the only orchestrator. It sends commands and reacts to events. The three participants only know their own command/event topics.

---

## 2. Saga happy path

```mermaid
sequenceDiagram
    actor Client
    participant Order as order-service
    participant K as Kafka
    participant Pay as payment-service
    participant Inv as inventory-service
    participant Del as delivery-service

    Client->>Order: POST /orders
    Note over Order: TX: save order(PENDING)<br/>+ outbox: RequestPayment
    Order-->>Client: 201 Created

    Order->>K: payment-commands: RequestPayment
    K->>Pay: RequestPayment
    Note over Pay: TX: charge<br/>+ outbox: PaymentApproved
    Pay->>K: payment-events: PaymentApproved
    K->>Order: PaymentApproved

    Note over Order: TX: order(PAID)<br/>+ outbox: ReserveStock
    Order->>K: inventory-commands: ReserveStock
    K->>Inv: ReserveStock
    Note over Inv: TX: reserve<br/>+ outbox: StockReserved
    Inv->>K: inventory-events: StockReserved
    K->>Order: StockReserved

    Note over Order: TX: order(RESERVED)<br/>+ outbox: ScheduleDelivery
    Order->>K: delivery-commands: ScheduleDelivery
    K->>Del: ScheduleDelivery
    Note over Del: TX: schedule<br/>+ outbox: DeliveryScheduled
    Del->>K: delivery-events: DeliveryScheduled
    K->>Order: DeliveryScheduled

    Note over Order: TX: order(COMPLETED)
```

---

## 3. Compensation flow (failure at any step)

```mermaid
sequenceDiagram
    participant Order as order-service
    participant Pay as payment-service
    participant Inv as inventory-service
    participant Del as delivery-service

    Note over Order,Del: Three failure scenarios, all compensate in reverse order

    rect rgb(255, 240, 240)
        Note over Pay: Payment fails
        Pay->>Order: PaymentDeclined
        Note over Order: order(FAILED) — nothing to compensate
    end

    rect rgb(255, 240, 240)
        Note over Inv: Inventory fails (after payment)
        Inv->>Order: StockReservationFailed
        Order->>Pay: RefundPayment
        Pay->>Order: PaymentRefunded
        Note over Order: order(FAILED)
    end

    rect rgb(255, 240, 240)
        Note over Del: Delivery fails (after payment + inventory)
        Del->>Order: DeliveryFailed
        Order->>Inv: ReleaseStock
        Inv->>Order: StockReleased
        Order->>Pay: RefundPayment
        Pay->>Order: PaymentRefunded
        Note over Order: order(FAILED)
    end
```

---

## 4. Outbox + idempotency (per-service internals)

```mermaid
flowchart TB
    KafkaIn[(Kafka<br/>command topic)]
    KafkaOut[(Kafka<br/>event topic)]

    subgraph svc["A participant service"]
        direction TB

        subgraph t1["TX 1: handle incoming command"]
            direction TB
            Consumer[Kafka Consumer]
            Check{event_id in<br/>processed_events?}
            Skip[Skip — already processed]
            Domain[Domain logic]
            ProcInsert[INSERT processed_events<br/>event_id, processed_at]
            DomainWrite[UPDATE / INSERT<br/>domain table]
            OutboxInsert[INSERT outbox<br/>event_id, payload]
        end

        subgraph t2["Background: outbox poller (every 1s)"]
            direction TB
            Poller[Scheduled @1s]
            Read[SELECT FROM outbox<br/>WHERE published_at IS NULL<br/>LIMIT N]
            Pub[Kafka Producer]
            Mark[UPDATE outbox<br/>SET published_at = now]
        end

        Consumer --> Check
        Check -- already seen --> Skip
        Check -- new --> Domain
        Domain --> ProcInsert
        Domain --> DomainWrite
        Domain --> OutboxInsert

        Poller --> Read --> Pub --> Mark
    end

    KafkaIn --> Consumer
    Pub --> KafkaOut
```

**Both blocks are independent transactions:**
- **TX 1** writes the domain row + processed_events row + outbox row atomically. If any of these fails the whole thing rolls back — Kafka redelivers the command and we try again.
- **TX 2** is the poller: read unsent rows, publish to Kafka, mark sent. If publish succeeds but the mark fails, we'll re-publish — that's why every receiver checks `processed_events` first.

---

## 5. Conventions (all services must follow)

### Event envelope

Every Kafka message — command or event — uses this JSON shape:

```json
{
  "event_id":   "uuid-v4",
  "event_type": "PaymentApproved",
  "saga_id":    "uuid-v4",
  "occurred_at": "2026-05-05T12:34:56Z",
  "payload":    { ... event-specific ... }
}
```

- `event_id` — unique per emission, used for idempotency. Kafka redeliveries keep the same id.
- `saga_id` — equals the order id. Lets every service correlate logs and the orchestrator track state.
- `event_type` — string discriminator; consumers route on this.
- `payload` — the actual business data.

Use the **same JSON shape** in command topics and event topics — only `event_type` differs.

### Kafka topics

| Topic                | Producer    | Consumers     |
| -------------------- | ----------- | ------------- |
| `payment-commands`   | order       | payment       |
| `payment-events`     | payment     | order         |
| `inventory-commands` | order       | inventory     |
| `inventory-events`   | inventory   | order         |
| `delivery-commands` | order       | delivery      |
| `delivery-events`   | delivery    | order         |

Single partition per topic for the lab — keeps ordering simple. Auto-create enabled in dev compose.

### Standard tables (every service)

```sql
-- outbox: events not yet published to Kafka
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    aggregate_id  VARCHAR(64)  NOT NULL,        -- usually saga_id / order id
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;

-- processed_events: idempotency log for inbound messages
CREATE TABLE processed_events (
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

Each service has its own database, but the schema for these two tables is identical across all four.

### Saga states (order-service only)

```
PENDING            → RequestPayment sent
PAYMENT_REQUESTED  → waiting for PaymentApproved/Declined
PAID               → ReserveStock sent
STOCK_REQUESTED    → waiting for StockReserved/Failed
RESERVED           → ScheduleDelivery sent
DELIVERY_REQUESTED → waiting for DeliveryScheduled/Failed
COMPLETED          → terminal success
FAILED             → terminal failure
COMPENSATING       → mid-rollback (sub-states optional)
```

### Service ports (host)

| Service     | App port | DB port |
| ----------- | -------- | ------- |
| order       | 8080     | 5432    |
| payment     | 8081     | 5433    |
| inventory   | 8082     | 5434    |
| delivery    | 8083     | 5435    |

Kafka exposed on `localhost:9092`.

### No shared library

Each service owns its own copy of event DTOs. This matches real-world microservices where services evolve schemas independently. Schema drift is acceptable — the JSON envelope is the contract.
