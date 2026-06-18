package service

import (
	"context"
	"fmt"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/producer"
)

type CommandService struct {
	repository domain.CommandRepository
	publisher  producer.CommandPublisher
}

func NewCommandService(repository domain.CommandRepository, publisher producer.CommandPublisher) *CommandService {
	return &CommandService{
		repository: repository,
		publisher:  publisher,
	}
}

func (s *CommandService) Submit(ctx context.Context, carID string, commandType domain.CommandType, payload string) (domain.RemoteCommand, error) {
	command := domain.RemoteCommand{
		ID:         fmt.Sprintf("command-%d", time.Now().UnixNano()),
		CarID:      carID,
		Type:       commandType,
		Payload:    payload,
		Status:     domain.CommandStatusPending,
		RetryCount: 0,
	}

	err := s.repository.Insert(ctx, command)
	if err != nil {
		return domain.RemoteCommand{}, err
	}

	err = s.publisher.Publish(ctx, command)
	if err != nil {
		return command, err
	}

	command.Status = domain.CommandStatusPublished

	err = s.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusPublished)
	if err != nil {
		return command, err
	}

	return command, nil
}
