package producer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/tracing"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
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

	// Build the message first so we can inject trace context into its headers.
	msg := kafka.Message{
		Key:   []byte(command.CarID),
		Value: value,
	}

	// Inject the current trace context into the Kafka message headers.
	// The propagator writes "traceparent" (and friends) into the headers,
	// so the consumer can continue this same trace.
	otel.GetTextMapPropagator().Inject(ctx,
		tracing.NewKafkaHeaderCarrier(&msg.Headers),
	)

	start := time.Now()

	err = p.writer.WriteMessages(ctx, msg)

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
