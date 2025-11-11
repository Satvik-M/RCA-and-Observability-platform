package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"ai-analysis-service/models"

	"github.com/segmentio/kafka-go"
	"google.golang.org/genai"
)

type App struct {
	kafkaReader *kafka.Reader
	genaiClient *genai.Client
}

func (app *App) analyzeError(entry models.LogEntry) string {
	fmt.Printf("Sending to Gemini for analysis: '%s'\n", entry.Message)

	// 1. Create a specific prompt
	prompt := fmt.Sprintf("You are an expert system administrator. Analyze the following log error and provide a brief, one-paragraph explanation of the potential cause and a recommended action. Log Error: %s", entry.Message)

	// 2. Send the request
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := app.genaiClient.Models.GenerateContent(
		ctx,
		"gemini-2.5-flash", // Using 1.5-flash, a fast and capable model
		genai.Text(prompt),
		nil,
	)
	if err != nil {
		log.Printf("Failed to generate content: %v", err)
		return "Error: Failed to contact AI service."
	}

	// 3. Extract and return the AI's text response
	return result.Text()
}

func main() {
	ctx := context.Background()
	client, err := genai.NewClient(ctx, nil) // nil automatically uses env var
	if err != nil {
		log.Fatalf("Failed to create genai client: %v", err)
	}

	time.Sleep(10 * time.Second)
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"kafka:29092"},
		Topic:    "errors-for-ai",
		GroupID:  "ai-analyzers",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	fmt.Println("Kafka service subscribed for topic 'errors-for-ai'")

	app := &App{
		kafkaReader: kafkaReader,
		genaiClient: client,
	}

	go func() {
		fmt.Println("Starting the go routine for handling the errors")

		for {
			m, err := app.kafkaReader.ReadMessage(context.Background())
			if err != nil {
				fmt.Println("Error reading message from Kafka:", err)
				continue
			}

			var entry models.LogEntry
			if err := json.Unmarshal(m.Value, &entry); err != nil {
				fmt.Println("Error unmarshalling log entry:", err)
				continue
			}

			aiAnalyzerResult := app.analyzeError(entry)
			fmt.Println("AI analysis result for log entry:", aiAnalyzerResult)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	fmt.Println("Shutting down AI analysis service")

	if err := app.kafkaReader.Close(); err != nil {
		fmt.Println("Error closing Kafka reader:", err)
	}

	fmt.Println("Service stopped cleanly")
}
