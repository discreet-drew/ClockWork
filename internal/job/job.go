package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusCreated Status = "CREATED"
	StatusQueued  Status = "QUEUED"
	StatusLeased  Status = "LEASED"
	StatusRunning Status = "RUNNING"
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusRetry   Status = "RETRY"
	StatusDead    Status = "DEAD"
)

var validTransitions = map[Status][]Status{
	StatusCreated: {StatusQueued},
	StatusQueued:  {StatusLeased},
	StatusLeased:  {StatusRunning, StatusQueued},
	StatusRunning: {StatusSuccess, StatusFailed},
	StatusFailed:  {StatusRetry, StatusDead},
	StatusRetry:   {StatusQueued},
}

func CanTransition(from, to Status) bool {
	for _, allowed := range validTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

type Job struct {
	ID             uuid.UUID
	WorkflowID     *uuid.UUID
	TaskName       string
	DependsOn      []string
	JobType        string
	Payload        json.RawMessage
	Status         Status
	RunAt          time.Time
	Attempts       int
	MaxAttempts    int
	IdempotencyKey *string
	LockedBy       *string
	LeaseExpiresAt *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func New(jobType string, payload json.RawMessage, opts ...Option) *Job {
	j := &Job{
		ID:          uuid.New(),
		JobType:     jobType,
		Payload:     payload,
		Status:      StatusCreated,
		RunAt:       time.Now().UTC(),
		MaxAttempts: 5,
		DependsOn:   []string{},
	}
	for _, opt := range opts {
		opt(j)
	}
	return j
}

type Option func(*Job)

func WithRunAt(t time.Time) Option       { return func(j *Job) { j.RunAt = t.UTC() } }
func WithMaxAttempts(n int) Option       { return func(j *Job) { j.MaxAttempts = n } }
func WithIdempotencyKey(k string) Option { return func(j *Job) { j.IdempotencyKey = &k } }

func (j *Job) MarkFailed(errMsg string, backoff func(attempt int) time.Duration) error {
	if !CanTransition(j.Status, StatusFailed) {
		return fmt.Errorf("invalid transition %s -> %s", j.Status, StatusFailed)
	}
	j.Status = StatusFailed
	j.LastError = &errMsg
	j.Attempts++

	if j.Attempts >= j.MaxAttempts {
		j.Status = StatusDead
		return nil
	}
	j.Status = StatusRetry
	j.RunAt = time.Now().UTC().Add(backoff(j.Attempts))
	return nil
}
