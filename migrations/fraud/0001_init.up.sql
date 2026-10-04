CREATE TABLE fraud_checks (
    payment_id    uuid        PRIMARY KEY,
    user_id       uuid        NOT NULL,
    amount_paise  bigint      NOT NULL CHECK (amount_paise > 0),
    approved      boolean     NOT NULL,
    reason        text        NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX fraud_checks_user_time_idx ON fraud_checks (user_id, created_at DESC);