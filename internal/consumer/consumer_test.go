package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/segmentio/kafka-go"
)

type fakeCommandRepository struct {
	processed map[string]bool
	statuses  map[string]domain.CommandStatus

	markProcessedErr error
	updateStatusErr  error
}

func (f *fakeCommandRepository) Insert(ctx context.Context, command domain.RemoteCommand) error {
	return nil
}

func (f *fakeCommandRepository) UpdateStatus(ctx context.Context, id string, status domain.CommandStatus) error {
	if f.updateStatusErr != nil {
		return f.updateStatusErr
	}
	f.statuses[id] = status
	return nil
}

func (f *fakeCommandRepository) MarkProcessed(ctx context.Context, commandID string) (bool, error) {
	if f.markProcessedErr != nil {
		return false, f.markProcessedErr
	}

	if f.processed[commandID] {
		return false, nil
	}

	f.processed[commandID] = true
	return true, nil
}

type fakeCar struct {
	sendCount int
	sendErr   error
}

func (f *fakeCar) Send(ctx context.Context, command domain.RemoteCommand) error {
	f.sendCount++
	return f.sendErr
}

type slowCar struct {
	delay     time.Duration
	sendCount int
}

func (s *slowCar) Send(ctx context.Context, command domain.RemoteCommand) error {
	s.sendCount++
	select {
	case <-time.After(s.delay):
		return nil // car responded in time
	case <-ctx.Done():
		return ctx.Err() // timeout fired first
	}
}

func TestConsumer_IdempotencySkipsDuplicateCommand(t *testing.T) {
	repo := &fakeCommandRepository{
		processed: make(map[string]bool),
		statuses:  make(map[string]domain.CommandStatus),
	}

	car := &fakeCar{}

	consumer := NewConsumer(nil, repo, car, 5*time.Second)

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

	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("first process returned error: %v", err)
	}

	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("second process returned error: %v", err)
	}

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}

	if repo.statuses[command.ID] != domain.CommandStatusAcknowledged {
		t.Fatalf("final status = %s, want %s", repo.statuses[command.ID], domain.CommandStatusAcknowledged)
	}
}

func TestConsumer_Process_CarOfflineReturnsNilAndMarksFailed(t *testing.T) {
	repo := &fakeCommandRepository{
		processed: make(map[string]bool),
		statuses:  make(map[string]domain.CommandStatus),
	}

	car := &fakeCar{
		sendErr: errors.New("car is offline"),
	}

	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	command := domain.RemoteCommand{
		ID:         "command-456",
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

	err = consumer.process(context.Background(), msg)
	if err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}

	if repo.statuses[command.ID] != domain.CommandStatusFailed {
		t.Fatalf("status = %s, want %s", repo.statuses[command.ID], domain.CommandStatusFailed)
	}
}

func TestConsumer_Process_MarkProcessedErrorReturnsError(t *testing.T) {
	repo := &fakeCommandRepository{
		processed:        make(map[string]bool),
		statuses:         make(map[string]domain.CommandStatus),
		markProcessedErr: errors.New("database unavailable"),
	}

	car := &fakeCar{}

	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	command := domain.RemoteCommand{
		ID:         "command-789",
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

	err = consumer.process(context.Background(), msg)
	if err == nil {
		t.Fatal("process returned nil, want error")
	}

	if car.sendCount != 0 {
		t.Fatalf("car.Send called %d times, want 0", car.sendCount)
	}
}

func TestConsumer_Process_MalformedJSONReturnsNil(t *testing.T) {
	repo := &fakeCommandRepository{
		processed: make(map[string]bool),
		statuses:  make(map[string]domain.CommandStatus),
	}

	car := &fakeCar{}

	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	msg := kafka.Message{
		Key:   []byte("car-001"),
		Value: []byte("{invalid-json"),
	}

	err := consumer.process(context.Background(), msg)
	if err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	if car.sendCount != 0 {
		t.Fatalf("car.Send called %d times, want 0", car.sendCount)
	}
}

func TestConsumer_Process_CarTimeoutMarksFailed(t *testing.T) {
	repo := &fakeCommandRepository{
		processed: make(map[string]bool),
		statuses:  make(map[string]domain.CommandStatus),
	}

	// car takes 200ms, but the timeout is 50ms — the timeout wins
	car := &slowCar{delay: 200 * time.Millisecond}

	consumer := NewConsumer(nil, repo, car, 50*time.Millisecond)

	command := domain.RemoteCommand{
		ID:    "command-timeout",
		CarID: "car-001",
		Type:  domain.CommandStartClimate,
	}
	value, _ := json.Marshal(command)
	msg := kafka.Message{Key: []byte(command.CarID), Value: value}

	err := consumer.process(context.Background(), msg)
	if err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	if repo.statuses[command.ID] != domain.CommandStatusFailed {
		t.Fatalf("status = %s, want FAILED", repo.statuses[command.ID])
	}
}
