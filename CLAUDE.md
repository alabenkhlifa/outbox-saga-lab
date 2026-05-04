# Architect-Training

Personal learning project to strengthen hands-on knowledge of distributed-systems patterns by building a small e-commerce food-ordering platform end-to-end. The goal is **understanding**, not production-readiness — code should stay readable and the patterns should be visible at a glance.

## Purpose

Build the system from scratch so each pattern is implemented and observable in isolation:

- **Microservices** with independent databases (database-per-service).
- **Outbox pattern** for reliable event publishing.
- **Saga orchestration** with compensating transactions.
- **Idempotent consumers** to handle Kafka redeliveries.

When introducing or modifying a pattern, prefer the textbook implementation over a clever shortcut. Comments and naming should make the pattern obvious — this is study code.

## Services

Four independent Spring Boot services, each with its own PostgreSQL database:

| Service     | Responsibility                                      |
| ----------- | --------------------------------------------------- |
| `order`     | Saga orchestrator. Owns order lifecycle.            |
| `payment`   | Authorizes / refunds payment.                       |
| `inventory` | Reserves / releases stock.                          |
| `delivery`  | Schedules / cancels delivery.                       |

Order Service drives the saga. Payment, Inventory, and Delivery are participants and emit reply events.

## Saga flow (happy path)

`OrderCreated` → `PaymentRequested` → `PaymentApproved` → `StockReservationRequested` → `StockReserved` → `DeliveryScheduleRequested` → `DeliveryScheduled` → `OrderCompleted`

## Compensations (reverse order on failure)

- Delivery fails → release stock → refund payment → mark order failed.
- Inventory fails → refund payment → mark order failed.
- Payment fails → mark order failed.

Each participant must own its compensating action and emit a confirmation event.

## Outbox pattern (every service)

- Domain change + outbox row in the **same DB transaction**.
- Background poller (`@Scheduled`) reads unpublished rows, publishes to Kafka, marks them sent.
- Outbox table: `id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload`, `created_at`, `published_at`.

Never publish to Kafka directly from business code. Always go through the outbox.

## Idempotency

- Every consumer persists processed `event_id`s in a `processed_events` table (PK on `event_id`).
- Check-then-insert inside the consumer transaction; skip processing on conflict.
- Producers must set a stable `event_id` (UUID) on outbox rows.

## Tech stack

- Java 21, Spring Boot 3.x, Spring Data JPA, Spring Kafka.
- PostgreSQL 16 (one instance per service).
- Apache Kafka + Zookeeper (single broker, local only).
- Docker Compose for the whole local stack.
- **Gradle** (Kotlin DSL, `build.gradle.kts`) per service. Always use the wrapper (`./gradlew`) — system Gradle is 7.5.1 and won't work with Java 21.

## Project layout (target)

```
architect-training/
├── docker-compose.yml
├── order-service/
├── payment-service/
├── inventory-service/
├── delivery-service/
└── common/                 # shared event DTOs, base outbox entity (optional)
```

Each service is independently runnable (`./gradlew bootRun`) and has its own `application.yml`, migrations, and Dockerfile.

## REST endpoints (minimum to demo the flow)

- `POST /orders` — create order, kicks off saga.
- `GET  /orders/{id}` — view current saga state.
- Admin/debug endpoints to inspect outbox + processed-events tables are welcome.

## Working agreements for Claude

- **Readability over cleverness.** This is study code — favor explicit, verbose, well-named implementations.
- **One pattern per change.** Don't bundle outbox + saga + idempotency into the same commit; it makes the learning fuzzy.
- **Show the seams.** Keep producers, consumers, outbox poller, saga orchestrator, and compensation handlers in clearly named, separate classes.
- **Don't introduce production concerns** (Kubernetes, service mesh, distributed tracing, complex retry libraries) unless explicitly asked. They obscure the patterns.
- **Use plain Spring Kafka** — no Spring Cloud Stream, no Axon, no third-party saga frameworks. The point is to see the mechanics.
- **Tests are welcome but optional**; integration tests with Testcontainers are preferred over heavy mocking when added.
- **Migrations** via Flyway, one folder per service.
- **Logging**: log every published event, every consumed event, and every saga state transition.
- **No local `psql` / `kcat`.** Inspect Postgres via `docker exec -it <pg-container> psql -U <user> -d <db>` and Kafka via the broker container's bundled scripts (`kafka-console-consumer.sh`, `kafka-topics.sh`). Don't suggest installing host-side clients.

## Out of scope (for now)

- Auth / users.
- Real payment provider — stub it (random approve/decline).
- Frontend.
- Multi-broker Kafka, schema registry, Avro/Protobuf — JSON is fine.
- Production-grade error handling, retry/backoff tuning, DLQs (basic only).
