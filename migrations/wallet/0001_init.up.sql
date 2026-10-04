-- Money is ALWAYS an integer in paise (1 INR = 100 paise). Never floats.

CREATE TABLE wallets (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        UNIQUE,
    kind        text        NOT NULL DEFAULT 'USER'
                            CHECK (kind IN ('USER', 'SYSTEM')),
    balance     bigint      NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT owner_matches_kind CHECK ((kind = 'USER') = (user_id IS NOT NULL)),
    CONSTRAINT balance_non_negative CHECK (kind = 'SYSTEM' OR balance >= 0)
);

-- id = payment id, so the same payment can never be applied twice.
CREATE TABLE transfers (
    id              uuid        PRIMARY KEY,
    from_wallet_id  uuid        NOT NULL REFERENCES wallets (id),
    to_wallet_id    uuid        NOT NULL REFERENCES wallets (id),
    amount          bigint      NOT NULL CHECK (amount > 0),
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT different_wallets CHECK (from_wallet_id <> to_wallet_id)
);

-- Written in the SAME transaction as the balance change; a relay sends it to Kafka later.
CREATE TABLE outbox (
    id            bigserial   PRIMARY KEY,
    topic         text        NOT NULL,
    key           text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;