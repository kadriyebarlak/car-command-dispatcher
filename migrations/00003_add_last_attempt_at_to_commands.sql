-- +goose Up
ALTER TABLE commands
ADD COLUMN last_attempt_at TIMESTAMP NULL;

-- +goose Down
ALTER TABLE commands
DROP COLUMN last_attempt_at;