package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type LogEntry struct {
	Level     string `json:"level"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

func analyzeError(entry LogEntry) string {
	fmt.Println("Analyzing error log entry:", entry)
	time.Sleep(100 * time.Millisecond) // Simulate analysis time

	var analysis string
	if strings.Contains(entry.Message, "database connection") {
		analysis = "Potential Cause: Possibly database connection issues"
	} else if strings.Contains(entry.Message, "invalid credentials") {
		analysis = "Potential Cause: The service is using an incorrect username or password. Recommendation: Verify environment variables for credentials."
	} else {
		analysis = "Potential Cause: A general application error occurred. Recommendation: Review application logic and surrounding logs for context."
	}
	return analysis
}

func main() {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"kafka:29092"},
		Topic:    "errors-for-ai",
		GroupID:  "ai-analyzers",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	fmt.Println("Kafka service subscribed for topic 'errors-for-ai'")

	go func() {
		fmt.Println("Starting the go routine for handling the errors")

		for {
			m, err := kafkaReader.ReadMessage(context.Background())
			if err != nil {
				fmt.Println("Error reading message from Kafka:", err)
				continue
			}

			var entry LogEntry
			if err := json.Unmarshal(m.Value, &entry); err != nil {
				fmt.Println("Error unmarshalling log entry:", err)
				continue
			}

			aiAnalyzerResult := analyzeError(entry)
			fmt.Println("AI analysis result for log entry:", aiAnalyzerResult)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	fmt.Println("Shutting down AI analysis service")

	if err := kafkaReader.Close(); err != nil {
		fmt.Println("Error closing Kafka reader:", err)
	}

	fmt.Println("Service stopped cleanly")
}
