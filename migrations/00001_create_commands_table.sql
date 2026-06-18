-- +goose Up
CREATE TABLE commands (
    id TEXT PRIMARY KEY,
    car_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE commands;