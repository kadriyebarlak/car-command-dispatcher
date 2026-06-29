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

// fakeCommandRepository implements domain.CommandRepository for tests.
// It tracks the idempotency state per command id and the latest command status,
// and lets each method's error be injected to test the failure paths.
type fakeCommandRepository struct {
	// idempotency state: "" (no row) | "PROCESSING" | "DONE"
	claimState map[string]string
	// latest command status written via UpdateStatus / MarkFailed / MarkAcknowledgedAndDone
	statuses map[string]domain.CommandStatus

	tryClaimErr       error
	updateStatusErr   error
	markFailedErr     error
	markAckAndDoneErr error
}

func newFakeRepo() *fakeCommandRepository {
	return &fakeCommandRepository{
		claimState: make(map[string]string),
		statuses:   make(map[string]domain.CommandStatus),
	}
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

// TryClaim mirrors the real upsert logic:
// no row -> insert PROCESSING -> return true
// PROCESSING -> return true (retry or unfinished redelivery)
// DONE -> return false (already completed)
func (f *fakeCommandRepository) TryClaim(ctx context.Context, commandID string) (bool, error) {
	if f.tryClaimErr != nil {
		return false, f.tryClaimErr
	}

	state := f.claimState[commandID]
	switch state {
	case "DONE":
		return false, nil
	case "PROCESSING":
		return true, nil
	default: // no row yet
		f.claimState[commandID] = "PROCESSING"
		return true, nil
	}
}

func (f *fakeCommandRepository) MarkDone(ctx context.Context, commandID string) error {
	f.claimState[commandID] = "DONE"
	return nil
}

func (f *fakeCommandRepository) MarkFailed(ctx context.Context, id string, attemptedAt time.Time) error {
	if f.markFailedErr != nil {
		return f.markFailedErr
	}
	f.statuses[id] = domain.CommandStatusFailed
	// note: claimState stays PROCESSING — this is what allows a later retry
	return nil
}

func (f *fakeCommandRepository) MarkAcknowledgedAndDone(ctx context.Context, id string) error {
	if f.markAckAndDoneErr != nil {
		return f.markAckAndDoneErr
	}
	f.statuses[id] = domain.CommandStatusAcknowledged
	f.claimState[id] = "DONE"
	return nil
}

type fakeCar struct {
	sendCount int
	sendErr   error
}

func (f *fakeCar) Send(ctx context.Context, command domain.RemoteCommand) error {
	f.sendCount++
	return f.sendErr
}

// slowCar respects the context deadline so the timeout path can be tested.
type slowCar struct {
	delay     time.Duration
	sendCount int
}

func (s *slowCar) Send(ctx context.Context, command domain.RemoteCommand) error {
	s.sendCount++
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func marshalMsg(t *testing.T, command domain.RemoteCommand) kafka.Message {
	t.Helper()
	value, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}
	return kafka.Message{Key: []byte(command.CarID), Value: value}
}

func sampleCommand(id string) domain.RemoteCommand {
	return domain.RemoteCommand{
		ID:         id,
		CarID:      "car-001",
		Type:       domain.CommandStartClimate,
		Payload:    "22C",
		Status:     domain.CommandStatusPublished,
		RetryCount: 0,
	}
}

func TestConsumer_HappyPath_AcknowledgedAndDone(t *testing.T) {
	repo := newFakeRepo()
	car := &fakeCar{}
	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	cmd := sampleCommand("command-ok")
	msg := marshalMsg(t, cmd)

	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}
	if repo.statuses[cmd.ID] != domain.CommandStatusAcknowledged {
		t.Fatalf("status = %s, want ACKNOWLEDGED", repo.statuses[cmd.ID])
	}
	if repo.claimState[cmd.ID] != "DONE" {
		t.Fatalf("claim state = %s, want DONE", repo.claimState[cmd.ID])
	}
}

func TestConsumer_DoneCommandIsSkipped(t *testing.T) {
	repo := newFakeRepo()
	car := &fakeCar{}
	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	cmd := sampleCommand("command-dup")
	msg := marshalMsg(t, cmd)

	// process twice — second delivery must be skipped because state is DONE
	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("first process returned error: %v", err)
	}
	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("second process returned error: %v", err)
	}

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}
}

func TestConsumer_CarOffline_MarksFailedAndCommits(t *testing.T) {
	repo := newFakeRepo()
	car := &fakeCar{sendErr: errors.New("car is offline")}
	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	cmd := sampleCommand("command-offline")
	msg := marshalMsg(t, cmd)

	// car offline is an outcome, not an infra failure -> process returns nil (commit)
	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}

	if car.sendCount != 1 {
		t.Fatalf("car.Send called %d times, want 1", car.sendCount)
	}
	if repo.statuses[cmd.ID] != domain.CommandStatusFailed {
		t.Fatalf("status = %s, want FAILED", repo.statuses[cmd.ID])
	}
	// claim must stay PROCESSING so the retry can go through later
	if repo.claimState[cmd.ID] != "PROCESSING" {
		t.Fatalf("claim state = %s, want PROCESSING (so retry is allowed)", repo.claimState[cmd.ID])
	}
}

func TestConsumer_FailedCommandCanBeRetried(t *testing.T) {
	repo := newFakeRepo()
	consumer := NewConsumer(nil, repo, &fakeCar{sendErr: errors.New("offline")}, 5*time.Second)

	cmd := sampleCommand("command-retry")
	msg := marshalMsg(t, cmd)

	// first attempt: car offline -> FAILED, claim stays PROCESSING
	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("first process returned error: %v", err)
	}

	// simulate the poller re-publishing: a new consumer, car now online
	onlineCar := &fakeCar{}
	retryConsumer := NewConsumer(nil, repo, onlineCar, 5*time.Second)
	if err := retryConsumer.process(context.Background(), msg); err != nil {
		t.Fatalf("retry process returned error: %v", err)
	}

	// the retry must NOT be skipped — claim was PROCESSING, so it reprocesses
	if onlineCar.sendCount != 1 {
		t.Fatalf("retry car.Send called %d times, want 1 (retry must not be skipped)", onlineCar.sendCount)
	}
	if repo.statuses[cmd.ID] != domain.CommandStatusAcknowledged {
		t.Fatalf("status = %s, want ACKNOWLEDGED after retry", repo.statuses[cmd.ID])
	}
}

func TestConsumer_TryClaimError_ReturnsErrorNoCommit(t *testing.T) {
	repo := newFakeRepo()
	repo.tryClaimErr = errors.New("database unavailable")
	car := &fakeCar{}
	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	msg := marshalMsg(t, sampleCommand("command-claimfail"))

	err := consumer.process(context.Background(), msg)
	if err == nil {
		t.Fatal("process returned nil, want error")
	}
	if car.sendCount != 0 {
		t.Fatalf("car.Send called %d times, want 0", car.sendCount)
	}
}

func TestConsumer_MalformedJSON_ReturnsNil(t *testing.T) {
	repo := newFakeRepo()
	car := &fakeCar{}
	consumer := NewConsumer(nil, repo, car, 5*time.Second)

	msg := kafka.Message{Key: []byte("car-001"), Value: []byte("{invalid-json")}

	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}
	if car.sendCount != 0 {
		t.Fatalf("car.Send called %d times, want 0", car.sendCount)
	}
}

func TestConsumer_CarTimeout_MarksFailed(t *testing.T) {
	repo := newFakeRepo()
	car := &slowCar{delay: 200 * time.Millisecond} // slower than the timeout
	consumer := NewConsumer(nil, repo, car, 50*time.Millisecond)

	msg := marshalMsg(t, sampleCommand("command-timeout"))

	if err := consumer.process(context.Background(), msg); err != nil {
		t.Fatalf("process returned error: %v", err)
	}
	if repo.statuses["command-timeout"] != domain.CommandStatusFailed {
		t.Fatalf("status = %s, want FAILED", repo.statuses["command-timeout"])
	}
}
