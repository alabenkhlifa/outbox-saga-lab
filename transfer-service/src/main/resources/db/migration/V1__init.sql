-- transfer-service initial schema
-- Saga aggregate + cross-cutting outbox/processed_events. Schemas of the
-- last two are identical across all four services.

CREATE TABLE transfer (
    id                  UUID            PRIMARY KEY,
    sender_user         VARCHAR(64)     NOT NULL,
    sender_currency     VARCHAR(3)         NOT NULL,
    recipient_user      VARCHAR(64)     NOT NULL,
    recipient_currency  VARCHAR(3)         NOT NULL,
    source_amount       NUMERIC(19, 4)  NOT NULL,
    target_amount       NUMERIC(19, 4),
    rate                NUMERIC(19, 8),
    state               VARCHAR(32)     NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX transfer_state_idx ON transfer (state);

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
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
