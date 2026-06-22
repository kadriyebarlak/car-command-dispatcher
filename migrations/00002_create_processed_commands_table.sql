-- +goose Up
CREATE TABLE processed_commands (
    command_id TEXT PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE processed_commands;