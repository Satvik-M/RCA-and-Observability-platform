package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type LogEntry struct {
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	logChan = make(chan LogEntry, 100)
)

func startLogWriterWorkers(logChannel <-chan LogEntry, wg *sync.WaitGroup, db *sql.DB) {
	worker := 3
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for logMsg := range logChannel {
				fmt.Printf("Worker %d processing log: %s\n", i, logMsg.Message)
				processLogIntoDb(logMsg, db)
			}
		}(i)
	}
}

func processLogIntoDb(logMsg LogEntry, db *sql.DB) {
	_, err := db.Exec("INSERT INTO logs (level, message, timestamp) VALUES ($1, $2, $3)", logMsg.Level, logMsg.Message, logMsg.Timestamp)
	if err != nil {
		fmt.Printf("Failed to insert log in Db: %v\n", err)
	}
}

func handleAddLog(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid JOSN", http.StatusBadRequest)
		return
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	select {
	case logChan <- entry:
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Log queued successfully")
	default:
		http.Error(w, "Server busy, try again later", http.StatusServiceUnavailable)
	}
}

func handleGetLog(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT level, message, timestamp FROM logs ORDER BY timestamp DESC LIMIT 50")
	if err != nil {
		fmt.Println("Error while getting logs", err)
		http.Error(w, "Error while getting logs", http.StatusInternalServerError)
		return
	}

	defer rows.Close()
	var logs []LogEntry
	for rows.Next() {
		var entry LogEntry
		if err := rows.Scan(&entry.Level, &entry.Message, &entry.Timestamp); err != nil {
			fmt.Println("Error while scanning logs", err)
			http.Error(w, "Error while scanning logs", http.StatusInternalServerError)
			return
		}
		logs = append(logs, entry)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func main() {
	connectionString := "postgres://satvikm:@localhost/satvikm?sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping the database: %v", err)
	}
	fmt.Println("Connected to the database successfully")

	// --- Ensure table exists ---
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS logs (
			id SERIAL PRIMARY KEY,
			level TEXT,
			message TEXT,
			timestamp TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetLog(db, w, r)
		case http.MethodPost:
			handleAddLog(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	var wg sync.WaitGroup
	startLogWriterWorkers(logChan, &wg, db)

	// start HTTP server in a goroutine
	srv := &http.Server{Addr: ":8080"}
	go func() {
		fmt.Println("Log processing service running on port 8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	fmt.Println("Shutting down...")

	// gracefully shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	// close log channel so workers exit
	close(logChan)
	wg.Wait()
	fmt.Println("Service stopped cleanly")

}
