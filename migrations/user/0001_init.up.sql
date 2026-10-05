CREATE TABLE profiles (
    user_id     uuid        PRIMARY KEY,
    email       text        NOT NULL,
    full_name   text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);