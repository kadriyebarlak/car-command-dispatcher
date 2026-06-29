package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
)

type PostgresCommandRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresCommandRepository(pool *pgxpool.Pool) *PostgresCommandRepository {
	return &PostgresCommandRepository{pool: pool}
}

func (r *PostgresCommandRepository) Insert(ctx context.Context, command domain.RemoteCommand) error {
	_, err := r.pool.Exec(ctx,
		"INSERT INTO commands (id, car_id, type, payload, status, retry_count) VALUES ($1, $2, $3, $4, $5, $6)",
		command.ID,
		command.CarID,
		command.Type,
		command.Payload,
		command.Status,
		command.RetryCount,
	)
	return err
}

func (r *PostgresCommandRepository) UpdateStatus(ctx context.Context, id string, status domain.CommandStatus) error {
	_, err := r.pool.Exec(ctx,
		"UPDATE commands SET status = $1, updated_at = NOW() WHERE id = $2",
		status, id,
	)
	return err
}

/*
func (r *PostgresCommandRepository) MarkProcessed(ctx context.Context, commandID string) (bool, error) {
	_, err := r.pool.Exec(ctx,
		"INSERT INTO processed_commands (command_id) VALUES ($1)",
		commandID,
	)
	if err == nil {
		return true, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return false, nil
	}

	return false, err
}
*/

func (r *PostgresCommandRepository) TryClaim(ctx context.Context, commandID string) (bool, error) {
	var state string

	err := r.pool.QueryRow(ctx, `
		INSERT INTO processed_commands (command_id, state)
		VALUES ($1, 'PROCESSING')
		ON CONFLICT (command_id) DO UPDATE
		SET state = processed_commands.state
		RETURNING state
	`, commandID).Scan(&state)

	if err != nil {
		return false, err
	}

	if state == "DONE" {
		return false, nil
	}

	return true, nil
}

func (r *PostgresCommandRepository) MarkDone(ctx context.Context, commandID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE processed_commands
		SET state = 'DONE',
		    processed_at = NOW()
		WHERE command_id = $1
	`, commandID)

	return err
}

func (r *PostgresCommandRepository) MarkFailed(ctx context.Context, id string, attemptedAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE commands
		 SET status = $1,
		     last_attempt_at = $2,
		     updated_at = NOW()
		 WHERE id = $3`,
		domain.CommandStatusFailed,
		attemptedAt,
		id,
	)
	return err
}

func (r *PostgresCommandRepository) MarkAcknowledgedAndDone(ctx context.Context, id string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE commands
		SET status = $1,
		    updated_at = NOW()
		WHERE id = $2
	`, domain.CommandStatusAcknowledged, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE processed_commands
		SET state = 'DONE',
		    processed_at = NOW()
		WHERE command_id = $1
	`, id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

var _ domain.CommandRepository = (*PostgresCommandRepository)(nil)
