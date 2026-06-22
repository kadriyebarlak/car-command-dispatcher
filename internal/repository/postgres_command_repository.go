package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
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

var _ domain.CommandRepository = (*PostgresCommandRepository)(nil)
