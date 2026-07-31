package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/car"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/consumer"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/handler"
	appmetrics "github.com/kadriyebarlak/car-command-dispatcher/internal/metrics"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/producer"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/repository"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/retry"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/service"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/tracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	// FIRST thing — set up structured logging before any log call
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("car command dispatcher starting...")

	appMetrics := appmetrics.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTracing, err := tracing.Init(ctx, "car-command-dispatcher", "localhost:4317")
	if err != nil {
		logger.Error("failed to init tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			logger.Error("failed to shut down tracing", "error", err)
		}
	}()

	pool, err := pgxpool.New(ctx, "postgres://notify:notify@localhost:5432/car_commands?sslmode=disable")
	if err != nil {
		log.Fatal("cannot create database pool:", err)
	}

	if err := pool.Ping(ctx); err != nil {
		log.Fatal("cannot connect to database:", err)
	}

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "car-commands",
		BatchSize: 1,
		//BatchTimeout: 10 * time.Millisecond,
	})

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "car-commands",
		GroupID: "car-command-consumer",
	})

	commandRepository := repository.NewPostgresCommandRepository(pool)
	commandPublisher := producer.NewKafkaPublisher(kafkaWriter)
	commandService := service.NewCommandService(commandRepository, commandPublisher, logger, appMetrics)
	commandHandler := handler.NewCommandHandler(commandService, logger, appMetrics)

	carSimulator := car.NewCarSimulator(0.9)
	commandConsumer := consumer.NewConsumer(kafkaReader, commandRepository, carSimulator, 5*time.Second, logger, appMetrics)
	commandConsumer.Start(ctx)

	// retry poller: re-publishes FAILED commands once their backoff has elapsed,
	// and marks them DEAD after maxRetries. Reuses the same publisher (same topic).
	retryPoller := retry.NewPoller(
		commandRepository,
		commandPublisher,
		3,              // maxRetries
		30*time.Second, // poll interval
		1*time.Second,  // backoff base
		30*time.Second, // backoff cap
		logger,
		appMetrics,
	)
	retryPoller.Start(ctx)

	r := chi.NewRouter()

	r.Post("/commands", commandHandler.CreateCommand)
	r.Handle("/metrics", promhttp.Handler())

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	tracedHandler := otelhttp.NewHandler(
		r,
		"http.server",
		otelhttp.WithSpanNameFormatter(
			func(_ string, req *http.Request) string {
				return req.Method + " " + req.URL.Path
			},
		),
	)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: tracedHandler,
	}

	go func() {
		log.Println("server listening on 8080")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error:", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("shutdown signal received")

	// cancel the root context — stops the consumer loop and the retry poller loop
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	if err := kafkaReader.Close(); err != nil {
		log.Printf("kafka reader close error: %v", err)
	}

	if err := kafkaWriter.Close(); err != nil {
		log.Printf("kafka writer close error: %v", err)
	}

	pool.Close()

	log.Println("shutdown complete")
}
