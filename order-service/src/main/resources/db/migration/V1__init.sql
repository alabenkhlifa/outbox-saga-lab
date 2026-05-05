-- =========================================================================
-- order-service initial schema
-- =========================================================================
-- Two domain tables (orders + order_items) plus the two cross-cutting
-- pattern tables (outbox, processed_events). The pattern tables match the
-- conventions in docs/architecture.md exactly so every service shares the
-- same schema for them.
-- =========================================================================

-- -------------------------------------------------------------------------
-- Domain: orders
-- -------------------------------------------------------------------------
CREATE TABLE orders (
    id            UUID PRIMARY KEY,
    status        VARCHAR(32)   NOT NULL,
    customer_id   VARCHAR(64)   NOT NULL,
    total_amount  NUMERIC(12,2) NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX orders_status_idx ON orders (status);

-- -------------------------------------------------------------------------
-- Domain: order_items
-- -------------------------------------------------------------------------
CREATE TABLE order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    UUID         NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku         VARCHAR(64)  NOT NULL,
    quantity    INT          NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);

-- -------------------------------------------------------------------------
-- Outbox: events not yet published to Kafka
-- (schema must match docs/architecture.md exactly)
-- -------------------------------------------------------------------------
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

-- -------------------------------------------------------------------------
-- Processed events: idempotency log for inbound messages
-- (schema must match docs/architecture.md exactly)
-- -------------------------------------------------------------------------
CREATE TABLE processed_events (
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
