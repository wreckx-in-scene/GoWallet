CREATE TABLE notifications (
    id          bigserial   PRIMARY KEY,
    payment_id  uuid        NOT NULL,
    user_id     uuid        NOT NULL,
    kind        text        NOT NULL,
    status      text        NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    attempts    int         NOT NULL DEFAULT 0,
    last_error  text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz,
    CONSTRAINT notifications_once UNIQUE (payment_id, kind)
);