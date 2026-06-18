package repository

import (
	"context"

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

var _ domain.CommandRepository = (*PostgresCommandRepository)(nil)
