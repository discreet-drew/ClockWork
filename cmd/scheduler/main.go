package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"clockwork/internal/leader"
	"clockwork/internal/scheduler"
	"clockwork/internal/store"
)

const electionLockKey = 727100

func main() {
	dsn := getenv("DATABASE_URL", "postgres://postgres:devpass@localhost:5432/clockwork?sslmode=disable")

	s, err := store.Open(dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer s.Close()

	elector := leader.NewPGElector(s.DB(), electionLockKey)
	sched := scheduler.New(s, elector)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("clockwork scheduler starting, campaigning for leadership...")
	if err := sched.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("scheduler stopped with error: %v", err)
	}
	_ = elector.Resign(context.Background())
	log.Println("scheduler stopped")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}