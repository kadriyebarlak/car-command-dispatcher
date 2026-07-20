package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/metrics"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader      *kafka.Reader
	repository  domain.CommandRepository
	car         domain.Car
	sendTimeout time.Duration
	logger      *slog.Logger
	metrics     *metrics.Metrics
}

func NewConsumer(reader *kafka.Reader, repository domain.CommandRepository, car domain.Car, sendTimeout time.Duration, logger *slog.Logger, metrics *metrics.Metrics) *Consumer {
	return &Consumer{
		reader:      reader,
		repository:  repository,
		car:         car,
		sendTimeout: sendTimeout,
		logger:      logger,
		metrics:     metrics,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					c.logger.Info("consumer stopped", "reason", ctx.Err())
					return
				}
				c.logger.Error(
					"failed to fetch Kafka message",
					"error", err,
				)
				continue
			}

			if err := c.process(ctx, msg); err != nil {
				// could not record outcome — do NOT commit, let Kafka redeliver
				c.logger.Error(
					"message processing failed; message will not be committed",
					"error", err,
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
				)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error(
					"failed to commit Kafka message",
					"error", err,
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
				)
			}
		}
	}()
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var command domain.RemoteCommand

	if err := json.Unmarshal(msg.Value, &command); err != nil {
		c.logger.Error(
			"failed to unmarshal Kafka message",
			"error", err,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
		return nil
	}

	logger := c.logger.With(
		"command_id", command.ID,
		"car_id", command.CarID,
		"command_type", command.Type,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)
	logger.Info("received command")

	shouldProcess, err := c.repository.TryClaim(ctx, command.ID)
	if err != nil {
		logger.Error(
			"failed to claim command",
			"error", err,
		)
		return fmt.Errorf("try claim: %w", err)
	}

	if !shouldProcess {
		logger.Info(
			"command already completed; skipping duplicate delivery",
		)
		return nil
	}

	if err := c.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusSent); err != nil {
		logger.Error(
			"failed to update command status",
			"target_status", domain.CommandStatusSent,
			"error", err,
		)
		return fmt.Errorf("update status to SENT: %w", err)
	}

	sendCtx, cancel := context.WithTimeout(ctx, c.sendTimeout)
	sendStartedAt := time.Now()
	err = c.car.Send(sendCtx, command)
	sendDuration := time.Since(sendStartedAt)
	cancel()

	c.metrics.CarSendDuration.Observe(sendDuration.Seconds())

	if err != nil {
		logger.Warn(
			"failed to send command to car",
			"error", err,
			"send_duration_ms", sendDuration.Milliseconds(),
		)

		if updateErr := c.repository.MarkFailed(ctx, command.ID, time.Now()); updateErr != nil {
			logger.Error(
				"failed to mark command as failed",
				"target_status", domain.CommandStatusFailed,
				"original_error", err,
				"error", updateErr,
			)
			return fmt.Errorf("update status to FAILED: %w", updateErr)
		}

		c.metrics.CommandsTotal.WithLabelValues("failed").Inc()

		logger.Info(
			"command marked as failed",
			"send_duration_ms", sendDuration.Milliseconds(),
		)

		return nil
	}

	if err := c.repository.MarkAcknowledgedAndDone(ctx, command.ID); err != nil {
		logger.Error(
			"failed to mark command as acknowledged and done",
			"target_status", domain.CommandStatusAcknowledged,
			"send_duration_ms", sendDuration.Milliseconds(),
			"error", err,
		)
		return fmt.Errorf("mark acknowledged and done: %w", err)
	}

	c.metrics.CommandsTotal.WithLabelValues("acknowledged").Inc()

	logger.Info(
		"command acknowledged",
		"send_duration_ms", sendDuration.Milliseconds(),
	)
	return nil
}
