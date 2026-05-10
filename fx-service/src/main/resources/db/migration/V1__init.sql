-- fx-service initial schema

CREATE TABLE fx_rate (
    base_currency   VARCHAR(3)         NOT NULL,
    quote_currency  VARCHAR(3)         NOT NULL,
    rate            NUMERIC(19, 8)  NOT NULL,
    PRIMARY KEY (base_currency, quote_currency)
);

CREATE TABLE fx_trade (
    id              UUID            PRIMARY KEY,
    base_currency   VARCHAR(3)         NOT NULL,
    quote_currency  VARCHAR(3)         NOT NULL,
    base_amount     NUMERIC(19, 4)  NOT NULL,
    quote_amount    NUMERIC(19, 4)  NOT NULL,
    rate            NUMERIC(19, 8)  NOT NULL,
    status          VARCHAR(16)     NOT NULL,
    correlation_id  UUID            NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX fx_trade_correlation_idx ON fx_trade (correlation_id);

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
