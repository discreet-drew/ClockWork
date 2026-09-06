package queue

import (
	"context"
	"encoding/json"
	"fmt"
)

type Handler func(ctx context.Context, payload json.RawMessage) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

type PrintPayload struct {
	Message string `json:"message"`
}

func HandlePrint(ctx context.Context, payload json.RawMessage) error {
	var p PrintPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[job:print] %s\n", p.Message)
	return nil
}

type FlakyPayload struct {
	FailProbabilityPct int `json:"fail_probability_pct"`
}

func HandleFlaky(ctx context.Context, payload json.RawMessage) error {
	var p FlakyPayload
	_ = json.Unmarshal(payload, &p)
	if randPercent() < p.FailProbabilityPct {
		return fmt.Errorf("simulated failure")
	}
	return nil
}