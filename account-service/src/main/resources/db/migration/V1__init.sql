-- account-service initial schema

CREATE TABLE wallet (
    id          UUID            PRIMARY KEY,
    user_id     VARCHAR(64)     NOT NULL,
    currency    VARCHAR(3)         NOT NULL,
    balance     NUMERIC(19, 4)  NOT NULL,
    version     BIGINT          NOT NULL DEFAULT 0,
    UNIQUE (user_id, currency)
);

CREATE INDEX wallet_user_idx ON wallet (user_id);

CREATE TABLE wallet_movement (
    id              UUID            PRIMARY KEY,
    wallet_id       UUID            NOT NULL REFERENCES wallet(id),
    amount          NUMERIC(19, 4)  NOT NULL,
    type            VARCHAR(16)     NOT NULL,
    correlation_id  UUID            NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX wallet_movement_wallet_idx ON wallet_movement (wallet_id);
CREATE INDEX wallet_movement_correlation_idx ON wallet_movement (correlation_id);

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
