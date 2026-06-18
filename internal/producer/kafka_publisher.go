package producer

import (
	"context"
	"encoding/json"

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

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(command.CarID),
		Value: value,
	})
}

var _ CommandPublisher = (*KafkaPublisher)(nil)
