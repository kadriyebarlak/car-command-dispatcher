package domain

import "context"

type CommandRepository interface {
	Insert(ctx context.Context, command RemoteCommand) error
	UpdateStatus(ctx context.Context, id string, status CommandStatus) error
	MarkProcessed(ctx context.Context, commandID string) (bool, error)
}
