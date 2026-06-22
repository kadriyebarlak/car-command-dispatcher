package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/segmentio/kafka-go"
)

type fakeCommandRepository struct {
	processed map[string]bool
	statuses  map[string]domain.CommandStatus
}

func (f *fakeCommandRepository) Insert(ctx context.Context, command domain.RemoteCommand) error {
	return nil
}

func (f *fakeCommandRepository) UpdateStatus(ctx context.Context, id string, status domain.CommandStatus) error {
	f.statuses[id] = status
	return nil
}

func (f *fakeCommandRepository) MarkProcessed(ctx context.Context, commandID string) (bool, error) {
	if f.processed[commandID] {
		return false, nil
	}

	f.processed[commandID] = true
	return true, nil
}

type fakeCar struct {
	sendCount int
}

func (f *fakeCar) Send(ctx context.Context, command domain.RemoteCommand) error {
	f.sendCount++
	return nil
}

func TestConsumer_IdempotencySkipsDuplicateCommand(t *testing.T) {
	repo := &fakeCommandRepository{
		processed: make(map[string]bool),
		statuses:  make(map[string]domain.CommandStatus),
	}

	car := &fakeCar{}

	consumer := NewConsumer(nil, repo, car)

	command := domain.RemoteCommand{
		ID:         "command-123",
		CarID:      "car-001",
		Type:       domain.CommandStartClimate,
		Payload:    "22C",
		Status:     domain.CommandStatusPublished,
		RetryCount: 0,
	}

	value, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	msg := kafka.Message{
		Key:   []byte(command.CarID),
		Value: value,
	}

	consumer.process(context.Background(), msg)
	consumer.process(context.Background(), msg)

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}

	if repo.statuses[command.ID] != domain.CommandStatusAcknowledged {
		t.Fatalf("final status = %s, want %s", repo.statuses[command.ID], domain.CommandStatusAcknowledged)
	}
}
