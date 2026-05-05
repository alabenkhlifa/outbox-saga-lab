-- ---------------------------------------------------------------------------
-- payment-service initial schema
--
-- Three tables:
--   1. payments          — domain table
--   2. outbox            — events written in the same TX as a domain change,
--                          drained to Kafka by a background poller.
--   3. processed_events  — idempotency log for inbound commands.
--
-- The outbox and processed_events schemas are identical across all four
-- services in this lab. See docs/architecture.md section 5.
-- ---------------------------------------------------------------------------

CREATE TABLE payments (
    id           UUID         PRIMARY KEY,
    order_id     UUID         NOT NULL,
    amount       NUMERIC(19, 2) NOT NULL,
    currency     VARCHAR(3)   NOT NULL DEFAULT 'EUR',
    status       VARCHAR(32)  NOT NULL,            -- APPROVED / DECLINED / REFUNDED
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX payments_order_id_idx ON payments (order_id);


CREATE TABLE outbox (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    aggregate_id  VARCHAR(64)  NOT NULL,
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;


CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
