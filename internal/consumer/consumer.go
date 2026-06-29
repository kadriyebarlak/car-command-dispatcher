package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader      *kafka.Reader
	repository  domain.CommandRepository
	car         domain.Car
	sendTimeout time.Duration
}

func NewConsumer(reader *kafka.Reader, repository domain.CommandRepository, car domain.Car, sendTimeout time.Duration) *Consumer {
	return &Consumer{
		reader:      reader,
		repository:  repository,
		car:         car,
		sendTimeout: sendTimeout,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("consumer: fetch error: %v", err)
				continue
			}

			if err := c.process(ctx, msg); err != nil {
				// could not record outcome — do NOT commit, let Kafka redeliver
				log.Printf("consumer: not committing, will redeliver: %v", err)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("consumer: commit failed: %v", err)
			}
		}
	}()
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var command domain.RemoteCommand

	if err := json.Unmarshal(msg.Value, &command); err != nil {
		log.Printf("consumer: failed to unmarshal message: %v", err)
		return nil
	}

	log.Printf("consumer: received command id=%s car_id=%s type=%s", command.ID, command.CarID, command.Type)

	shouldProcess, err := c.repository.TryClaim(ctx, command.ID)
	if err != nil {
		log.Printf("consumer: failed to claim command %s: %v", command.ID, err)
		return fmt.Errorf("try claim: %w", err)
	}

	if !shouldProcess {
		log.Printf("consumer: command %s already DONE, skipping", command.ID)
		return nil
	}

	if err := c.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusSent); err != nil {
		log.Printf("consumer: failed to update status to SENT for command %s: %v", command.ID, err)
		return fmt.Errorf("update status to SENT: %w", err)
	}

	sendCtx, cancel := context.WithTimeout(ctx, c.sendTimeout)
	err = c.car.Send(sendCtx, command)
	cancel()
	if err != nil {
		log.Printf("consumer: car send failed for command %s: %v", command.ID, err)

		if updateErr := c.repository.MarkFailed(ctx, command.ID, time.Now()); updateErr != nil {
			log.Printf("consumer: failed to update status to FAILED for command %s: %v", command.ID, updateErr)
			return fmt.Errorf("update status to FAILED: %w", updateErr)
		}

		return nil
	}

	if err := c.repository.MarkAcknowledgedAndDone(ctx, command.ID); err != nil {
		log.Printf("consumer: failed to mark command %s as ACKNOWLEDGED and DONE: %v", command.ID, err)
		return fmt.Errorf("mark acknowledged and done: %w", err)
	}

	log.Printf("consumer: command %s acknowledged", command.ID)
	return nil
}
