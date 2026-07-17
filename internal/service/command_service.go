package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/producer"
)

type CommandService struct {
	repository domain.CommandRepository
	publisher  producer.CommandPublisher
	logger     *slog.Logger
}

func NewCommandService(repository domain.CommandRepository, publisher producer.CommandPublisher, logger *slog.Logger) *CommandService {
	return &CommandService{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
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

	commandLogger := s.logger.With(
		"command_id", command.ID,
		"car_id", command.CarID,
		"command_type", command.Type,
	)

	commandLogger.Info("command received")

	err := s.repository.Insert(ctx, command)
	if err != nil {
		commandLogger.Error(
			"failed to insert command",
			"error", err,
		)
		return domain.RemoteCommand{}, err
	}

	err = s.publisher.Publish(ctx, command)
	if err != nil {
		commandLogger.Error(
			"failed to publish command",
			"error", err,
		)
		return command, err
	}

	command.Status = domain.CommandStatusPublished

	err = s.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusPublished)
	if err != nil {
		commandLogger.Error(
			"failed to update command status",
			"target_status", domain.CommandStatusPublished,
			"error", err,
		)
		return command, err
	}

	commandLogger.Info(
		"command published",
		"status", command.Status,
	)

	return command, nil
}
