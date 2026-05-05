-- =========================================================================
-- delivery-service initial schema
-- =========================================================================
-- One domain table (deliveries) plus the two cross-cutting pattern tables
-- (outbox, processed_events). The pattern tables match the conventions in
-- docs/architecture.md exactly so every service shares the same schema for
-- them.
-- =========================================================================

-- -------------------------------------------------------------------------
-- Domain: deliveries
-- -------------------------------------------------------------------------
-- One row per delivery. The status field is the saga-relevant lifecycle
-- (SCHEDULED / FAILED / CANCELLED). order_id is the saga correlation key.
CREATE TABLE deliveries (
    id            UUID PRIMARY KEY,
    order_id      UUID         NOT NULL,
    address       TEXT         NOT NULL,
    scheduled_at  TIMESTAMPTZ,
    status        VARCHAR(32)  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX deliveries_order_id_idx ON deliveries (order_id);
CREATE INDEX deliveries_status_idx   ON deliveries (status);

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
