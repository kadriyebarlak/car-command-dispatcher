package domain

import (
	"context"
	"time"
)

type CommandRepository interface {
	Insert(ctx context.Context, command RemoteCommand) error
	UpdateStatus(ctx context.Context, id string, status CommandStatus) error
	TryClaim(ctx context.Context, commandID string) (bool, error)
	MarkDone(ctx context.Context, commandID string) error
	MarkFailed(ctx context.Context, id string, attemptedAt time.Time) error
	MarkAcknowledgedAndDone(ctx context.Context, id string) error
}
