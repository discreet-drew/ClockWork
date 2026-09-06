package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"clockwork/internal/queue"
	"clockwork/internal/store"
)

func main() {
	dsn := getenv("DATABASE_URL", "postgres://postgres:devpass@localhost:5432/clockwork?sslmode=disable")
	numWorkers, _ := strconv.Atoi(getenv("NUM_WORKERS", "4"))
	workerIDBase := getenv("WORKER_ID_BASE", "worker")

	s, err := store.Open(dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer s.Close()

	registry := queue.NewRegistry()
	registry.Register("print", queue.HandlePrint)
	registry.Register("flaky", queue.HandleFlaky)

	pool := &queue.Pool{
		Store:         s,
		Registry:      registry,
		NumWorkers:    numWorkers,
		PollInterval:  500 * time.Millisecond,
		LeaseDuration: 30 * time.Second,
		WorkerIDBase:  workerIDBase,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("clockwork worker pool starting: %d workers", numWorkers)
	pool.Run(ctx)  
	log.Println("worker pool stopped")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}