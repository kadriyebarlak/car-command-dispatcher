package retry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/metrics"
)

type CommandPublisher interface {
	Publish(ctx context.Context, command domain.RemoteCommand) error
}

type Poller struct {
	repository domain.CommandRepository
	publisher  CommandPublisher
	logger     *slog.Logger
	metrics    *metrics.Metrics

	maxRetries int
	interval   time.Duration
	base       time.Duration
	cap        time.Duration
}

func NewPoller(
	repository domain.CommandRepository,
	publisher CommandPublisher,
	maxRetries int,
	interval time.Duration,
	base time.Duration,
	cap time.Duration,
	logger *slog.Logger,
	metrics *metrics.Metrics,
) *Poller {
	return &Poller{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
		metrics:    metrics,
		maxRetries: maxRetries,
		interval:   interval,
		base:       base,
		cap:        cap,
	}
}

// TODO: Track the poller goroutine with a WaitGroup and add a Stop method.
// Context cancellation tells the poller to stop, but main currently does not
// wait for it to finish before closing shared resources(the database pool and Kafka writer).
func (p *Poller) Start(ctx context.Context) {
	p.logger.Info(
		"retry poller starting",
		"interval", p.interval,
		"base_backoff", p.base,
		"backoff_cap", p.cap,
		"max_retries", p.maxRetries,
	)

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				p.logger.Info(
					"retry poller stopped",
					"reason", ctx.Err(),
				)
				return

			case <-ticker.C:
				if err := p.runOnce(ctx); err != nil {
					if ctx.Err() != nil {
						p.logger.Info(
							"retry poller stopped during run",
							"reason", ctx.Err(),
						)
						return
					}
					p.logger.Error(
						"retry poller run failed",
						"error", err,
					)
				}
			}
		}
	}()
}

func (p *Poller) runOnce(ctx context.Context) error {
	commands, err := p.repository.FindRetryable(ctx, p.maxRetries)
	if err != nil {
		return fmt.Errorf("find retryable commands: %w", err)
	}

	p.metrics.PendingRetries.Set(float64(len(commands)))

	p.logger.Info(
		"retryable commands found",
		"count", len(commands),
	)

	now := time.Now()

	for _, command := range commands {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		commandLogger := p.logger.With(
			"command_id", command.ID,
			"retry_count", command.RetryCount,
		)

		if command.LastAttemptAt == nil {
			commandLogger.Warn(
				"command has no last attempt time; skipping",
			)
			continue
		}

		delay := Backoff(command.RetryCount, p.base, p.cap)
		dueAt := command.LastAttemptAt.Add(delay)

		if now.Before(dueAt) {
			commandLogger.Debug(
				"command is not due for retry yet",
				"due_at", dueAt,
				"retry_in", dueAt.Sub(now).String(),
				"backoff", delay.String(),
			)
			continue
		}

		if command.RetryCount+1 >= p.maxRetries {
			commandLogger.Warn(
				"command exhausted retries; marking as dead",
				"max_retries", p.maxRetries,
			)

			if err := p.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusDead); err != nil {
				commandLogger.Error(
					"failed to mark command as dead",
					"target_status", domain.CommandStatusDead,
					"error", err,
				)
				continue
			}

			p.metrics.CommandsTotal.WithLabelValues("dead").Inc()

			commandLogger.Info(
				"command marked as dead",
				"status", domain.CommandStatusDead,
			)

			continue
		}

		newRetryCount := command.RetryCount + 1

		if err := p.repository.MarkForRetry(ctx, command.ID, newRetryCount); err != nil {
			commandLogger.Error(
				"failed to mark command for retry",
				"new_retry_count", newRetryCount,
				"error", err,
			)
			continue
		}

		command.RetryCount = newRetryCount
		command.Status = domain.CommandStatusPublished

		if err := p.publisher.Publish(ctx, command); err != nil {
			commandLogger.Error(
				"failed to publish retry command",
				"new_retry_count", newRetryCount,
				"error", err,
			)
			continue
		}

		p.metrics.RetriesTotal.Inc()

		commandLogger.Info(
			"command republished for retry",
			"new_retry_count", newRetryCount,
			"status", command.Status,
		)
	}

	return nil
}
