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

## Learning sessions

When walking the user through a distributed-systems pattern:

- **Research real-world adoption** via WebSearch. Look at primary engineering content (Medium / company tech blogs, conference talks, named-engineer LinkedIn posts). Default references to probe: Revolut, Monzo, Wise, N26, Stripe, Uber, Netflix, DoorDash, Shopify, Slack.
- **Produce a Notion-ready annex** when the search surfaces concrete numbers, named systems, or non-obvious design choices. Annex format: title + verification date, one-paragraph summary, ≥1 mermaid diagram, key facts (throughput / latency / persistence model / divergence from the textbook), why-non-obvious-choice (if any), interview talking points, sources as markdown hyperlinks. Wrap the annex in a fenced ````markdown` block so it copy-pastes into Notion cleanly.
- **Mermaid for Notion**: Notion's mermaid renderer lags upstream — use pipe-style edge labels `-->|"label"|`, not `-- "label" -->`. Never use empty labels (`-- "" -->`); plain `-->` is correct. Inside `subgraph X["..."]`, avoid parentheses and special punctuation in the display name. Use plain text after `<br/>` instead of parenthesized qualifiers.
- **Cite, don't speculate.** Label inferences as inferences. Skip the annex entirely if the search turns up nothing verifiable — don't fabricate numbers or pad.
- **Live-observe over lecture.** Distributed-systems concepts are invisible without watching the seams. When a user is learning a pattern, set up DB watch terminals first, then trigger scenarios — never batch-dump multiple snapshots in one tool call where the user can't observe state changing live.
- **Pre-action contract before every state-changing scenario** (transfers, compensations, chaos toggles, toxiproxy changes, traffic-sim). The pattern: (1) state in ONE sentence what you're about to do, (2) state in ONE sentence which terminal and which rows/columns the user should focus on, (3) WAIT for explicit approval. Don't proceed on assumed readiness, and don't batch multiple actions in one approval cycle — one action per cycle.
- **macOS-friendly DB watch one-liners.** macOS `watch` (from `brew install watch`) breaks on `psql` invocations with multiple `-c` flags — the second-and-later flags get mis-parsed. Use one `-c` with semicolon-joined SQL: `watch -n 1 'docker exec <db> psql -U <u> -d <d> -c "SELECT ...; SELECT ...; SELECT ..."'`. To run two `docker exec` calls in one watch frame, chain them with `;` inside the single-quoted argument.
- **Keep teaching answers short.** Lead with the answer in 1-3 sentences, then a tight table or 3-4 bullets max. No big "Notion-ready summary" blocks unless the user asks. No restating the question. If the user wants depth, they'll ask follow-ups.
- **Notion summaries: super concise or the user won't read them.** When the user asks for a Notion summary/recap of a session, default to **one short page**: one-line definition, one diagram (only if it earns its place), a tight "what I got wrong" list (≤5 bullets, one line each), and a one-liner A+ answer. No production-vs-lab tables, no interview talking points, no code references, no open-questions section, unless the user explicitly asks. Length target: fits on one screen without scrolling. The mistakes section is the highest-value part — emphasize it.
- **Feynman tests: one sub-question at a time, never batch them.** When grading the user's understanding of a pattern, ask **one** sub-question, let them answer, give a concise correction in plain words (1-3 sentences + tight table or ≤4 bullets), then move to the next. Do **not** ask all 3 sub-questions in one message — the user finds batched questions overwhelming and the corrections get muddled across topics. After each sub-question is sealed, explicitly say "ready for Q2?" before continuing. Save the overall grade for the very end.

## Out of scope (for now)

- Auth / users.
- Real payment provider — stub it (random approve/decline).
- Frontend.
- Multi-broker Kafka, schema registry, Avro/Protobuf — JSON is fine.
- Production-grade error handling, retry/backoff tuning, DLQs (basic only).
