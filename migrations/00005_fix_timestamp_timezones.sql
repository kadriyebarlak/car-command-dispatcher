-- +goose Up
ALTER TABLE commands
    ALTER COLUMN created_at      TYPE TIMESTAMPTZ USING created_at      AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at      TYPE TIMESTAMPTZ USING updated_at      AT TIME ZONE 'UTC',
    ALTER COLUMN last_attempt_at TYPE TIMESTAMPTZ USING last_attempt_at AT TIME ZONE 'UTC';

ALTER TABLE processed_commands
    ALTER COLUMN processed_at TYPE TIMESTAMPTZ USING processed_at AT TIME ZONE 'UTC';

-- +goose Down
ALTER TABLE commands
    ALTER COLUMN created_at      TYPE TIMESTAMP USING created_at      AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at      TYPE TIMESTAMP USING updated_at      AT TIME ZONE 'UTC',
    ALTER COLUMN last_attempt_at TYPE TIMESTAMP USING last_attempt_at AT TIME ZONE 'UTC';

ALTER TABLE processed_commands
    ALTER COLUMN processed_at TYPE TIMESTAMP USING processed_at AT TIME ZONE 'UTC';