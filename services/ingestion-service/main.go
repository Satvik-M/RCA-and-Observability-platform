package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type LogEntry struct {
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	kafkaWriter *kafka.Writer
)

func handleAddLog(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	logBytes, err := json.Marshal(entry)
	if err != nil {
		fmt.Println("Error marshalling log entry:", err)
	}

	msg := kafka.Message{
		Value: logBytes,
	}

	err = kafkaWriter.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("Error writing to kafka:", err)
	}
	w.WriteHeader(http.StatusOK)
	fmt.Println("Log entry added to kafka:", entry)
}

func main() {
	//connect to kafka
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "raw-logs",
		Balancer: &kafka.LeastBytes{},
	}

	fmt.Println("Connected to kafka successfully")

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handleAddLog(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	srv := &http.Server{
		Addr: ":8080",
	}
	go func() {
		fmt.Println("Listening on port 8080")
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Error starting server:", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	fmt.Println("Shutting down...")

	// --- Gracefully shutdown server ---
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	// --- Close Kafka writer ---
	if err := kafkaWriter.Close(); err != nil {
		log.Printf("Failed to close Kafka writer: %v", err)
	}
	fmt.Println("Service stopped cleanly")
}
