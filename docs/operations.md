# Operations

How to run, exercise, and debug the lab. All commands run from the repo root.

## 1. Bring the stack up

```bash
docker compose up -d
docker compose ps          # everything should be healthy after ~30s
```

The stack contains: zookeeper, kafka, toxiproxy, kafka-ui, and four Postgres instances (`transfers-db`, `accounts-db`, `fx-db`, `notifications-db`). Toxiproxy owns host port 9092 and forwards to Kafka with zero toxics by default.

## 2. Start the four Spring Boot services

In separate terminals (or with `gradle --console=plain bootRun &`):

```bash
(cd transfer-service     && ./gradlew bootRun)
(cd account-service      && ./gradlew bootRun)
(cd fx-service           && ./gradlew bootRun)
(cd notification-service && ./gradlew bootRun)
```

Each service uses its own Postgres on a different port. Flyway runs the migrations on first boot, including the `account-service` wallet seed and the `fx-service` rate seed.

## 3. Send a transfer

Single happy-path call:

```bash
curl -s -X POST http://localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{
    "senderUser":"alice",
    "senderCurrency":"EUR",
    "recipientUser":"bob",
    "recipientCurrency":"USD",
    "sourceAmount":"50.00"
  }' | jq
```

Response (201):

```json
{
  "id": "...",
  "senderUser": "alice",
  ...
  "state": "DEBIT_REQUESTED"
}
```

Inspect saga progress (state will advance through DEBIT_REQUESTED → SENDER_DEBITED → FX_REQUESTED → FX_CONVERTED → CREDIT_REQUESTED → RECIPIENT_CREDITED → NOTIFICATION_REQUESTED → COMPLETED):

```bash
curl -s http://localhost:8080/transfers/<id> | jq
```

## 4. Generate traffic

```bash
k6 run traffic-sim/scenarios.js
```

The default scenario sends 5 transfers/second for 30s across alice/bob/carol, picking from the seeded currency pairs.

## 5. Inspect Postgres (Docker exec)

```bash
# transfer-service: list recent transfers and their state
docker exec -it transfers-db psql -U transfers -d transfers \
  -c "SELECT id, sender_user, sender_currency, recipient_user, recipient_currency, source_amount, target_amount, state FROM transfer ORDER BY created_at DESC LIMIT 10;"

# account-service: show current wallet balances
docker exec -it accounts-db psql -U accounts -d accounts \
  -c "SELECT user_id, currency, balance FROM wallet ORDER BY user_id, currency;"

# account-service: recent movements for one transfer
docker exec -it accounts-db psql -U accounts -d accounts \
  -c "SELECT type, amount, correlation_id FROM wallet_movement WHERE correlation_id = '<transfer-id>' ORDER BY created_at;"

# fx-service: trades for one transfer
docker exec -it fx-db psql -U fx -d fx \
  -c "SELECT base_currency, quote_currency, base_amount, quote_amount, status FROM fx_trade WHERE correlation_id = '<transfer-id>';"

# notification-service: recent notifications
docker exec -it notifications-db psql -U notifications -d notifications \
  -c "SELECT user_id, kind, created_at FROM notification ORDER BY created_at DESC LIMIT 10;"
```

## 6. Inspect Kafka

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list

# Tail account.events
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic account.events \
  --from-beginning
```

Or use the GUI at http://localhost:8090 (kafka-ui).

## 7. Inject failures via Toxiproxy

`tools/chaos.sh` provides shortcuts on top of the Toxiproxy admin API at http://localhost:8474:

```bash
./tools/chaos.sh latency 500   # add 500ms one-way latency on the kafka proxy
./tools/chaos.sh jitter 200    # add ±200ms jitter
./tools/chaos.sh kill          # close the proxy entirely (consumers drop)
./tools/chaos.sh heal          # remove all toxics
```

While a toxic is active, send transfers and watch the saga: redeliveries should be skipped by `processed_events`, balances should be exact-once, and `outbox` rows should drain after the proxy is healed.

## 8. Trigger compensation paths

| Failure | How to trigger |
|---|---|
| `DebitRejected` | `sourceAmount` larger than the sender's wallet balance |
| `ConversionRejected` | use a currency pair NOT in `fx_rate` (e.g. `JPY` → `EUR`) |
| `CreditRejected` | `recipientCurrency` for which the recipient has no wallet (e.g. carol GBP) |

Inspect compensations via `GET /transfers/<id>` (state should be `FAILED`) and the participant tables (look for paired DEBIT + REFUND in `wallet_movement`, or paired EXECUTED + REVERSED in `fx_trade`).

## 9. Bring it down

```bash
docker compose down
docker compose down -v   # also drops volumes (resets seeded data)
```
