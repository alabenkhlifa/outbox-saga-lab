# Operations

How to bring the stack up, send traffic, inject failures, and inspect what's happening. The architecture of the system itself lives in `architecture.md` — this file is purely about running and observing it.

---

## Stack overview

`docker-compose.yml` brings up:

| Service       | Where it listens (host)        | Purpose                                    |
| ------------- | ------------------------------ | ------------------------------------------ |
| `zookeeper`   | `:2181`                        | Required by Kafka 7.5.x                    |
| `kafka`       | _in-network only_ (`:29092`)   | Single broker — host clients go via toxiproxy |
| `toxiproxy`   | `:9092` (kafka), `:8474` admin | Default transparent — chaos is opt-in      |
| `kafka-ui`    | `:8090`                        | Visual topic browser                       |
| `orders-db`   | `:5432`                        | postgres for order-service                 |
| `payments-db` | `:5433`                        | postgres for payment-service               |
| `inventory-db`| `:5434`                        | postgres for inventory-service             |
| `delivery-db` | `:5435`                        | postgres for delivery-service              |

The four Spring Boot services run on the host (not in compose) on `:8080`–`:8083`.

---

## Bring it up

```bash
docker compose up -d
docker compose ps                 # verify all healthy
```

Start each service in its own terminal — they take ~30s to be ready:

```bash
cd order-service     && ./gradlew bootRun
cd payment-service   && ./gradlew bootRun
cd inventory-service && ./gradlew bootRun
cd delivery-service  && ./gradlew bootRun
```

Bring it down (preserves volumes):

```bash
docker compose down
```

Bring it down and wipe data:

```bash
docker compose down -v
```

---

## Send traffic (k6)

Requires `brew install k6` (one-time).

```bash
k6 run traffic-sim/scenarios.js                       # all 3 scenarios at once
k6 run --env SCENARIO=diurnal  traffic-sim/scenarios.js
k6 run --env SCENARIO=burst    traffic-sim/scenarios.js
k6 run --env SCENARIO=hotSku   traffic-sim/scenarios.js
k6 run --env BASE_URL=http://localhost:8080 traffic-sim/scenarios.js
```

Scenarios:

- **`diurnal`** — ramping arrival rate over a compressed "day" with two peaks (lunch + dinner). Useful for watching the outbox poller catch up after surges.
- **`burst`** — flat 5 RPS, sudden spike to 100 RPS for 30s, then back. Watch Kafka consumer lag and outbox backlog grow then drain.
- **`hotSku`** — every order asks for `soda-cola` (seeded with only 5 units). Most orders fail at inventory and trigger the full compensation path. Best demo of the saga in action.

---

## Inject failures (toxiproxy)

`tools/chaos.sh` is a thin wrapper around the toxiproxy admin API on `:8474`:

```bash
./tools/chaos.sh status                # list active toxics
./tools/chaos.sh latency 2000          # add 2s latency on every byte
./tools/chaos.sh slow_close 500        # delay close by 500ms
./tools/chaos.sh bandwidth 1024        # throttle to 1 KB/s
./tools/chaos.sh down                  # disable proxy entirely (drop all kafka traffic)
./tools/chaos.sh up                    # re-enable
./tools/chaos.sh clear                 # remove all toxics, re-enable
```

Why toxiproxy is always in the host→Kafka path: see `architecture.md` §6.

### Suggested chaos experiments

| Goal                                  | Recipe                                                        |
| ------------------------------------- | ------------------------------------------------------------- |
| Validate idempotency under retries    | `latency 5000` while running diurnal — Kafka redelivers       |
| Validate outbox replay after blackout | `down`, run k6 for 30s, `up` — outbox drains the backlog      |
| Test compensation under contention    | Run `hotSku` scenario (no chaos needed)                       |
| Test recovery after service crash     | `docker compose stop kafka` then `start` mid-saga             |
| Test Postgres outage                  | `docker compose stop payments-db` then `start`                |

---

## Send a single order

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{
        "customerId": "cust-1",
        "items": [
          {"sku":"pizza-margherita","quantity":2,"unitPrice":12.50},
          {"sku":"soda-cola","quantity":1,"unitPrice":3.00}
        ]
      }'
```

The response includes `Location: /orders/<id>`.

---

## Inspect a saga

REST:

```bash
curl http://localhost:8080/orders/<id> | jq
```

Watch a saga's state every second:

```bash
watch -n 1 "curl -s http://localhost:8080/orders/<id> | jq '.status'"
```

Visual via Kafka UI: `http://localhost:8090` — pick the cluster, browse topics, view messages, watch consumer-group lag.

Logs (one terminal per service is best). All services log every consumed event, every state transition, every published event with `saga_id` for grep:

```bash
# In each service's terminal, the logs are already there.
# Or, if you redirected to a file:
grep saga_id=<uuid-fragment> ./logs/order-service.log
```

---

## Inspect databases

No host-side `psql` — go through the container. Same pattern for all four DBs:

```bash
docker exec -it orders-db    psql -U orders    -d orders
docker exec -it payments-db  psql -U payments  -d payments
docker exec -it inventory-db psql -U inventory -d inventory
docker exec -it delivery-db  psql -U delivery  -d delivery
```

### Useful one-liners

Outbox lag (how many events are waiting to publish):

```bash
docker exec -it orders-db psql -U orders -d orders \
  -c "SELECT count(*) FROM outbox WHERE published_at IS NULL"
```

Saga state distribution:

```bash
docker exec -it orders-db psql -U orders -d orders \
  -c "SELECT status, count(*) FROM orders GROUP BY status"
```

Recent processed events (per service, helpful for debugging idempotency):

```bash
docker exec -it payments-db psql -U payments -d payments \
  -c "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10"
```

Stock levels:

```bash
docker exec -it inventory-db psql -U inventory -d inventory \
  -c "SELECT * FROM stock"
```

---

## Inspect Kafka

The friendly way: `http://localhost:8090` (kafka-ui).

The raw way (run inside the kafka container):

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list

# Tail a topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic payment-events --from-beginning

# Describe consumer-group lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --all-groups
```

---

## REST endpoints (summary)

| Method | Path                      | Service     | Purpose                              |
| ------ | ------------------------- | ----------- | ------------------------------------ |
| POST   | `/orders`                 | order       | Create order, kick off saga          |
| GET    | `/orders/{id}`            | order       | Order + current saga state           |
| GET    | `/payments/by-order/{id}` | payment     | Debug: payment record for an order   |
| GET    | `/stock`                  | inventory   | Debug: current stock levels          |
| GET    | `/deliveries/by-order/{id}` | delivery  | Debug: delivery record for an order  |

All bodies / responses are JSON. The order-service is the only public surface; the rest are debug-only.
