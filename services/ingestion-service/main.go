package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

type LogEntry struct {
	Timestamp string
	Level     string
	Message   string
	Source    string
}

type LogCommand struct {
	Entry  LogEntry
	Result chan error
}

var (
	logFile = "logs.jsonl"
	logChan = make(chan LogCommand, 100)
)

func addHelper() {
	// f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Fatalf("Failed to open log file: %v", err)
	// }

	// defer f.Close()

	for cmd := range logChan {
		// bytes, _ := json.Marshal(cmd.Entry)
		// _, err := f.Write(bytes)
		// f.Write([]byte("\n"))
		// cmd.Result <- err
		fmt.Println(cmd.Entry)
	}
}

func handleAddLog(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid JOSN", http.StatusBadRequest)
		return
	}

	done := make(chan error)
	logChan <- LogCommand{Entry: entry, Result: done}

	if err := <-done; err != nil {
		http.Error(w, "Failed to write to the logs", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Println("Log added successfully")
}

func handleGetLog(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(logFile)
	if err != nil {
		http.Error(w, "No logs found", http.StatusNotFound)
		return
	}

	lines := []LogEntry{}
	for _, line := range splitLines(string(data)) {
		if line == "" {
			continue
		}
		var e LogEntry
		json.Unmarshal([]byte(line), &e)
		lines = append(lines, e)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(lines)

}

func splitLines(s string) []string {
	result := []string{}
	current := ""
	for _, ch := range s {
		if ch == '\n' {
			result = append(result, current)
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func main() {
	connectionString := "postgres://satvikm:@localhost/satvikm?sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()
	go addHelper()

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetLog(w, r)
		case http.MethodPost:
			handleAddLog(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("Log processing service running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
