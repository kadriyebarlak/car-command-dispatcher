package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/metrics"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/producer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("car-command-dispatcher/service")

type CommandService struct {
	repository domain.CommandRepository
	publisher  producer.CommandPublisher
	logger     *slog.Logger
	metrics    *metrics.Metrics
}

func NewCommandService(repository domain.CommandRepository, publisher producer.CommandPublisher, logger *slog.Logger, metrics *metrics.Metrics) *CommandService {
	return &CommandService{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
		metrics:    metrics,
	}
}

func (s *CommandService) Submit(ctx context.Context, carID string, commandType domain.CommandType, payload string) (domain.RemoteCommand, error) {
	ctx, span := tracer.Start(ctx, "CommandService.Submit")
	defer span.End()

	command := domain.RemoteCommand{
		ID:         fmt.Sprintf("command-%d", time.Now().UnixNano()),
		CarID:      carID,
		Type:       commandType,
		Payload:    payload,
		Status:     domain.CommandStatusPending,
		RetryCount: 0,
	}

	span.SetAttributes(
		attribute.String("command.id", command.ID),
		attribute.String("car.id", command.CarID),
		attribute.String("command.type", string(command.Type)),
	)

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

	s.metrics.CommandsTotal.WithLabelValues("published").Inc()

	commandLogger.Info(
		"command published",
		"status", command.Status,
	)

	return command, nil
}
