# outbox-saga-lab

Personal learning project — a small Revolut-style banking backend built end-to-end to strengthen hands-on knowledge of distributed-systems patterns. The goal is **understanding**, not production-readiness — code stays readable and the patterns must be visible at a glance.

The four implemented patterns:

- **Microservices** with database-per-service.
- **Outbox pattern** for reliable event publishing.
- **Saga orchestration** with compensating transactions.
- **Idempotent consumers** for Kafka redelivery.

When introducing or modifying a pattern, prefer the textbook implementation over a clever shortcut. Comments and naming should make the pattern obvious — this is study code.

The core flow is a **cross-currency peer-to-peer money transfer**: debit the sender's wallet, FX-convert, credit the recipient's wallet, notify both parties. Every leg is reversible, exercising the compensation chain.

---

## Documentation map

This file is the slim orientation. Detail lives in topic files — load on demand:

- **[`docs/architecture.md`](docs/architecture.md)** — **Use when** designing or modifying any pattern (saga states, outbox flow, idempotency, compensation routing), naming events, picking topic names, deciding table schemas, or producing a new diagram. Has the four mermaid diagrams plus the strict conventions all services must follow (event envelope, topic names, standard tables, ports, saga states, chaos topology).
- **[`docs/operations.md`](docs/operations.md)** — **Use when** bringing the stack up, sending traffic, injecting failures, debugging a saga, querying databases, or inspecting Kafka topics. Covers `docker compose`, k6, `tools/chaos.sh`, kafka-ui, the REST surface, and the most useful `docker exec` one-liners.

If both files seem relevant, `architecture.md` first (it's the contract), then `operations.md` (how to exercise it).

---

## Services

| Service                | Responsibility                                  | Port |
| ---------------------- | ----------------------------------------------- | ---- |
| `transfer-service`     | Saga orchestrator — owns the transfer lifecycle | 8080 |
| `account-service`      | Wallets per (user, currency); debit/credit/refund | 8081 |
| `fx-service`           | Currency conversion using a seeded rate table   | 8082 |
| `notification-service` | Records sent notifications (no real push)       | 8083 |

`transfer-service` drives the saga. `account-service`, `fx-service`, and `notification-service` are participants and emit reply events. The full happy-path + compensation flows are in `docs/architecture.md` §2–§3.

---

## Tech stack

- Java 21, Spring Boot 3.3.x, Spring Data JPA, plain Spring Kafka.
- PostgreSQL 16 — one instance per service.
- Apache Kafka + Zookeeper — single broker, local only.
- Toxiproxy in front of Kafka for chaos; kafka-ui for visual inspection.
- Docker Compose for the whole local stack.
- **Gradle** Kotlin DSL (`build.gradle.kts`) per service. Always use the wrapper (`./gradlew`) — system Gradle is 7.5.1 and won't work with Java 21.

## Project layout

```
outbox-saga-lab/
├── docker-compose.yml      # zookeeper, kafka, 4x postgres, toxiproxy, kafka-ui
├── docs/
│   ├── architecture.md     # diagrams + conventions (the contract)
│   └── operations.md       # running, traffic, chaos, debugging
├── transfer-service/       # saga orchestrator
├── account-service/        # saga participant — wallets
├── fx-service/             # saga participant — currency conversion
├── notification-service/   # saga participant — notifications
├── tools/
│   ├── chaos.sh            # toxiproxy shortcuts
│   └── toxiproxy/          # init config
└── traffic-sim/
    └── scenarios.js        # k6 traffic scenarios
```

Each service is independently runnable (`./gradlew bootRun`) with its own `application.yml` and Flyway migrations.

---

## Working agreements for Claude

- **Readability over cleverness.** Study code — favor explicit, verbose, well-named implementations.
- **One pattern per change.** Don't bundle outbox + saga + idempotency into the same commit; it makes the learning fuzzy.
- **Show the seams.** Producers, consumers, outbox poller, saga orchestrator, compensation handlers — clearly named, separate classes.
- **Don't introduce production concerns** (Kubernetes, service mesh, distributed tracing, complex retry libraries) unless explicitly asked. They obscure the patterns.
- **Use plain Spring Kafka** — no Spring Cloud Stream, no Axon, no third-party saga frameworks.
- **Tests are welcome but optional.** Integration tests with Testcontainers are preferred over heavy mocking when added.
- **Migrations** via Flyway, one folder per service.
- **Logging**: every published event, every consumed event, every saga state transition.
- **No local `psql` / `kcat`.** Inspect Postgres via `docker exec -it <pg-container> psql ...` and Kafka via the broker's bundled scripts. Don't suggest installing host-side clients.

## Out of scope (for now)

- Auth / users.
- Real payment provider — stub it (random approve/decline).
- Frontend.
- Multi-broker Kafka, schema registry, Avro/Protobuf — JSON is fine.
- Production-grade error handling, retry/backoff tuning, DLQs (basic only).
