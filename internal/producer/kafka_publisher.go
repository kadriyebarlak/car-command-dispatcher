package producer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/segmentio/kafka-go"
)

type CommandPublisher interface {
	Publish(ctx context.Context, command domain.RemoteCommand) error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(writer *kafka.Writer) *KafkaPublisher {
	return &KafkaPublisher{
		writer: writer,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, command domain.RemoteCommand) error {
	value, err := json.Marshal(command)
	if err != nil {
		return err
	}

	start := time.Now()

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(command.CarID),
		Value: value,
	})

	duration := time.Since(start)

	if err != nil {
		slog.Error(
			"kafka publish failed",
			"command_id", command.ID,
			"car_id", command.CarID,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
		return err
	}

	slog.Info(
		"kafka publish completed",
		"command_id", command.ID,
		"car_id", command.CarID,
		"duration_ms", duration.Milliseconds(),
	)

	return nil
}

var _ CommandPublisher = (*KafkaPublisher)(nil)
