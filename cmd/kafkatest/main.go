package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "car-commands",
	})
	defer writer.Close()

	err := writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte("car-001"),
			Value: []byte("START_CLIMATE"),
		},
	)
	if err != nil {
		log.Fatal("failed to write message:", err)
	}

	fmt.Println("message produced successfully")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "car-commands",
		GroupID: "car-dispatcher",
	})
	defer reader.Close()

	message, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatal("failed to read message:", err)
	}

	fmt.Printf("message consumed: key=%s value=%s\n", string(message.Key), string(message.Value))
}
