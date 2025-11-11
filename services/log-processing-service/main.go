package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/segmentio/kafka-go"
)

type LogEntry struct {
	Level     string `json:"level"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type App struct {
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
	esClient    *elasticsearch.Client
}

func initializeElasticsearch() *elasticsearch.Client {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://elasticsearch:9200"},
	})
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: " + err.Error())
	}

	fmt.Println("Elasticsearch client initialized")
	return es
}

func (app *App) processLogIntoElastic(logEntry LogEntry) {
	doc, _ := json.Marshal(logEntry)
	res, err := app.esClient.Index("logs", bytes.NewReader(doc))
	if err != nil {
		log.Fatalf("Error getting response from Elasticsearch: " + err.Error())
	}
	defer res.Body.Close()
	fmt.Println("Log entry indexed into Elasticsearch:", logEntry)
}

func (app *App) forwardErrorToKafka(lofEntry LogEntry) {
	logBytes, err := json.Marshal(lofEntry)
	if err != nil {
		fmt.Println("Error marshalling log entry for error forwarding:", err)
		return
	}

	msg := kafka.Message{
		Value: logBytes,
	}

	err = app.kafkaWriter.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("Error writing error log to kafka:", err)
		return
	}
	fmt.Println("Error log entry forwarded to Kafka:", lofEntry)
}

func main() {
	time.Sleep(10 * time.Second)
	elasticClient := initializeElasticsearch()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"kafka:29092"},
		Topic:    "raw-logs",
		GroupID:  "log-processors",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	writer := &kafka.Writer{
		Addr:     kafka.TCP("kafka:29092"),
		Topic:    "errors-for-ai",
		Balancer: &kafka.LeastBytes{},
	}

	app := &App{
		kafkaReader: reader,
		kafkaWriter: writer,
		esClient:    elasticClient,
	}
	fmt.Println("Kafka reader and writer initialized")

	fmt.Println("Log Processing Service started, waiting for messages...")

	fmt.Println("Starting to read messages from Kafka")
	go func() {
		for {
			m, err := app.kafkaReader.ReadMessage(context.Background())
			if err != nil {
				log.Fatalf("Error reading message from Kafka: " + err.Error())
			}

			var logEntry LogEntry
			if err := json.Unmarshal(m.Value, &logEntry); err != nil {
				fmt.Println("Error unmarshalling log entry:", err)
				continue
			}
			app.forwardErrorToKafka(logEntry)
			app.processLogIntoElastic(logEntry)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	fmt.Println("Shutting down log processing service")
	if err := app.kafkaReader.Close(); err != nil {
		log.Fatalf("%s", "Error closing Kafka reader: "+err.Error())
	}

	if err := app.kafkaWriter.Close(); err != nil {
		log.Fatalf("%s", "Error closing Kafka writer: "+err.Error())
	}
	// esClient.Close()
}
