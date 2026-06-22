package consumer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader     *kafka.Reader
	repository domain.CommandRepository
	car        domain.Car
}

func NewConsumer(reader *kafka.Reader, repository domain.CommandRepository, car domain.Car) *Consumer {
	return &Consumer{
		reader:     reader,
		repository: repository,
		car:        car,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go func() {
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Println("consumer: context cancelled, stopping")
					return
				}

				log.Printf("consumer: read error: %v", err)
				continue
			}

			c.process(ctx, msg)
		}
	}()
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) {
	var command domain.RemoteCommand

	if err := json.Unmarshal(msg.Value, &command); err != nil {
		log.Printf("consumer: failed to unmarshal message: %v", err)
		return
	}

	log.Printf("consumer: received command id=%s car_id=%s type=%s", command.ID, command.CarID, command.Type)

	firstTime, err := c.repository.MarkProcessed(ctx, command.ID)
	if err != nil {
		log.Printf("consumer: failed to mark command %s as processed: %v", command.ID, err)
		return
	}

	if !firstTime {
		log.Printf("consumer: duplicate command %s, skipping", command.ID)
		return
	}

	if err := c.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusSent); err != nil {
		log.Printf("consumer: failed to update status to SENT for command %s: %v", command.ID, err)
		return
	}

	if err := c.car.Send(ctx, command); err != nil {
		log.Printf("consumer: car send failed for command %s: %v", command.ID, err)

		if updateErr := c.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusFailed); updateErr != nil {
			log.Printf("consumer: failed to update status to FAILED for command %s: %v", command.ID, updateErr)
		}

		return
	}

	if err := c.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusAcknowledged); err != nil {
		log.Printf("consumer: failed to update status to ACKNOWLEDGED for command %s: %v", command.ID, err)
		return
	}

	log.Printf("consumer: command %s acknowledged", command.ID)
}
