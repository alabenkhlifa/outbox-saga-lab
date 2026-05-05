-- inventory-service schema
-- Conventions: outbox + processed_events tables match docs/architecture.md exactly,
-- so all four services share the same idempotency + outbox layout.

-- ---------------------------------------------------------------------------
-- Domain: stock
-- One row per SKU with the currently available quantity. Decremented on
-- ReserveStock and incremented back on ReleaseStock.
-- ---------------------------------------------------------------------------
CREATE TABLE stock (
    sku           VARCHAR(64)  PRIMARY KEY,
    available_qty INT          NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT stock_qty_non_negative CHECK (available_qty >= 0)
);

-- ---------------------------------------------------------------------------
-- Domain: reservations
-- One row per (order_id, sku) pair. Status transitions:
--   RESERVED -> RELEASED (compensation)
--   FAILED   (terminal — written when a reserve attempt found insufficient stock)
-- ---------------------------------------------------------------------------
CREATE TABLE reservations (
    id         UUID         PRIMARY KEY,
    order_id   UUID         NOT NULL,
    sku        VARCHAR(64)  NOT NULL REFERENCES stock(sku),
    qty        INT          NOT NULL,
    status     VARCHAR(16)  NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT reservations_status_chk CHECK (status IN ('RESERVED','RELEASED','FAILED'))
);

CREATE INDEX reservations_order_idx ON reservations (order_id);

-- ---------------------------------------------------------------------------
-- Outbox: events not yet published to Kafka.
-- Schema is identical across all services — see docs/architecture.md §5.
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    aggregate_id  VARCHAR(64)  NOT NULL,        -- usually saga_id / order id
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Processed events: idempotency log for inbound Kafka messages.
-- Identical schema across all services — see docs/architecture.md §5.
-- ---------------------------------------------------------------------------
CREATE TABLE processed_events (
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
