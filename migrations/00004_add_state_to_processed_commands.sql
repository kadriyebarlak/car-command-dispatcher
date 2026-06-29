-- +goose Up
ALTER TABLE processed_commands
ADD COLUMN state TEXT NOT NULL DEFAULT 'DONE';

ALTER TABLE processed_commands
ALTER COLUMN state SET DEFAULT 'PROCESSING';

ALTER TABLE processed_commands
ADD CONSTRAINT processed_commands_state_check
CHECK (state IN ('PROCESSING', 'DONE'));

-- +goose Down
ALTER TABLE processed_commands
DROP CONSTRAINT processed_commands_state_check;

ALTER TABLE processed_commands
DROP COLUMN state;