CREATE TABLE payments (
    id              uuid        PRIMARY KEY,
    user_id         uuid        NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    from_wallet_id  uuid        NOT NULL,
    to_wallet_id    uuid        NOT NULL,
    amount_paise    bigint      NOT NULL CHECK (amount_paise > 0),
    status          text        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','FRAUD_APPROVED','COMPLETED','REJECTED','FAILED')),
    failure_reason  text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payments_idempotency UNIQUE (user_id, idempotency_key),
    CONSTRAINT payments_different_wallets CHECK (from_wallet_id <> to_wallet_id)
);

CREATE INDEX payments_open_idx ON payments (updated_at)
    WHERE status IN ('PENDING', 'FRAUD_APPROVED');

CREATE TABLE outbox (
    id            bigserial   PRIMARY KEY,
    topic         text        NOT NULL,
    key           text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;