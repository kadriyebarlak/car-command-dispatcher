package main

import (
	"context"
	"log"
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
	"github.com/kadriyebarlak/car-command-dispatcher/internal/producer"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/repository"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/service"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.Println("car command dispatcher starting...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://notify:notify@localhost:5432/car_commands?sslmode=disable")
	if err != nil {
		log.Fatal("cannot create database pool:", err)
	}

	if err := pool.Ping(ctx); err != nil {
		log.Fatal("cannot connect to database:", err)
	}

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "car-commands",
	})

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "car-commands",
		GroupID: "car-command-consumer",
	})

	commandRepository := repository.NewPostgresCommandRepository(pool)
	commandPublisher := producer.NewKafkaPublisher(kafkaWriter)
	commandService := service.NewCommandService(commandRepository, commandPublisher)
	commandHandler := handler.NewCommandHandler(commandService)

	carSimulator := car.NewCarSimulator(0)
	commandConsumer := consumer.NewConsumer(kafkaReader, commandRepository, carSimulator, 5*time.Second)
	commandConsumer.Start(ctx)

	r := chi.NewRouter()

	r.Post("/commands", commandHandler.CreateCommand)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
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
