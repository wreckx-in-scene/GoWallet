CREATE TABLE ledger_entries (
    id               bigserial   PRIMARY KEY,
    transfer_id      uuid        NOT NULL,
    wallet_id        uuid        NOT NULL,
    counterparty_id  uuid        NOT NULL,
    direction        text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_paise     bigint      NOT NULL CHECK (amount_paise > 0),
    balance_after    bigint      NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ledger_entry_once UNIQUE (transfer_id, wallet_id)
);

CREATE INDEX ledger_entries_wallet_idx ON ledger_entries (wallet_id, id);