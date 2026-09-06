package main

import (
	"log"
	"net/http"
	"os"
	"clockwork/internal/api"
	"clockwork/internal/store"
)

func main() {
	dsn := getenv("DATABASE_URL", "postgres://postgres:devpass@localhost:5432/clockwork?sslmode=disable")
	addr := getenv("API_ADDR", ":8080")

	s, err := store.Open(dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer s.Close()

	srv := api.New(s)
	log.Printf("clockwork api listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
