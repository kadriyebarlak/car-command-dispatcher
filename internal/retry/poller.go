package retry

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
)

type CommandPublisher interface {
	Publish(ctx context.Context, command domain.RemoteCommand) error
}

type Poller struct {
	repository domain.CommandRepository
	publisher  CommandPublisher

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
) *Poller {
	return &Poller{
		repository: repository,
		publisher:  publisher,
		maxRetries: maxRetries,
		interval:   interval,
		base:       base,
		cap:        cap,
	}
}

func (p *Poller) Start(ctx context.Context) {
	log.Printf("retry poller: starting (interval=%s base=%s cap=%s maxRetries=%d)",
		p.interval, p.base, p.cap, p.maxRetries)

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("retry poller: context cancelled, stopping")
				return

			case <-ticker.C:
				if err := p.runOnce(ctx); err != nil {
					log.Printf("retry poller: run failed: %v", err)
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

	log.Printf("retry poller: tick — found %d retryable command(s)", len(commands))

	now := time.Now()

	for _, command := range commands {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if command.LastAttemptAt == nil {
			log.Printf("retry poller: command %s has no last_attempt_at, skipping", command.ID)
			continue
		}

		delay := Backoff(command.RetryCount, p.base, p.cap)
		dueAt := command.LastAttemptAt.Add(delay)

		if now.Before(dueAt) {
			log.Printf("retry poller: command %s not due yet (due in %s)", command.ID, time.Until(dueAt))
			continue
		}

		if command.RetryCount+1 >= p.maxRetries {
			log.Printf("retry poller: command %s exhausted retries, marking DEAD", command.ID)

			if err := p.repository.UpdateStatus(ctx, command.ID, domain.CommandStatusDead); err != nil {
				log.Printf("retry poller: failed to mark command %s as DEAD: %v", command.ID, err)
				continue
			}

			continue
		}

		newRetryCount := command.RetryCount + 1

		if err := p.repository.MarkForRetry(ctx, command.ID, newRetryCount); err != nil {
			log.Printf("retry poller: failed to mark command %s for retry: %v", command.ID, err)
			continue
		}

		command.RetryCount = newRetryCount
		command.Status = domain.CommandStatusPublished

		if err := p.publisher.Publish(ctx, command); err != nil {
			log.Printf("retry poller: failed to publish retry command %s: %v", command.ID, err)
			continue
		}

		log.Printf("retry poller: republished command %s retry_count=%d", command.ID, newRetryCount)
	}

	return nil
}
