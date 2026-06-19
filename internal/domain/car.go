package domain

import "context"

type Car interface {
	Send(ctx context.Context, command RemoteCommand) error
}
