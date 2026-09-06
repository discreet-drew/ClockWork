// Package api implements Clockwork's HTTP surface (a simplified take on
// Phase 16 -- production concerns like auth, pagination, and versioning
// are called out as TODOs rather than implemented, to keep this
// buildable in one sitting; see the README's "what's deliberately not
// here yet" section).
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"clockwork/internal/job"
	"clockwork/internal/store"
)

type Server struct {
	Store *store.Store
	mux   *http.ServeMux
}

func New(s *store.Store) *Server {
	srv := &Server{Store: s, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleCreateJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("POST /workflows", s.handleCreateWorkflow)
	s.mux.HandleFunc("GET /health", s.handleHealth)
}

type createJobRequest struct {
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
}

// handleCreateJob: POST /jobs
// TODO (Phase 16/17, not implemented here): request size limits, auth,
// rate limiting, and stricter payload validation before this touches a
// real network.
func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.JobType == "" {
		http.Error(w, "job_type is required", http.StatusBadRequest)
		return
	}
	var opts []job.Option
	if req.RunAt != nil {
		opts = append(opts, job.WithRunAt(*req.RunAt))
	}
	if req.IdempotencyKey != "" {
		opts = append(opts, job.WithIdempotencyKey(req.IdempotencyKey))
	}
	if req.MaxAttempts > 0 {
		opts = append(opts, job.WithMaxAttempts(req.MaxAttempts))
	}
	j := job.New(req.JobType, req.Payload, opts...)

	id, err := s.Store.Enqueue(r.Context(), j)
	if err != nil {
		http.Error(w, "failed to enqueue job: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id.String()})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}
	row := s.Store.DB().QueryRowContext(r.Context(),
		`SELECT id, status, attempts, max_attempts, last_error FROM jobs WHERE id=$1`, id)

	var (
		jobID       uuid.UUID
		status      string
		attempts    int
		maxAttempts int
		lastError   *string
	)
	if err := row.Scan(&jobID, &status, &attempts, &maxAttempts, &lastError); err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id": jobID, "status": status, "attempts": attempts,
		"max_attempts": maxAttempts, "last_error": lastError,
	})
}

type createWorkflowRequest struct {
	Name  string        `json:"name"`
	Tasks []taskRequest `json:"tasks"`
}

type taskRequest struct {
	Name      string          `json:"name"`
	JobType   string          `json:"job_type"`
	Payload   json.RawMessage `json:"payload"`
	DependsOn []string        `json:"depends_on"`
}

// handleCreateWorkflow: POST /workflows
// Submits a full DAG at once: every task is inserted up front (Phase 11),
// and Store.Claim's dependency-gate query is what actually enforces
// ordering -- there's no separate "workflow engine loop" walking the
// graph. This keeps the DAG execution logic in one place (the SQL query)
// instead of duplicating graph-walking logic in application code.
func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req createWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Name == "" || len(req.Tasks) == 0 {
		http.Error(w, "name and at least one task are required", http.StatusBadRequest)
		return
	}

	wfID, err := s.Store.CreateWorkflow(r.Context(), req.Name)
	if err != nil {
		http.Error(w, "failed to create workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	taskIDs := make(map[string]string)
	for _, t := range req.Tasks {
		j := job.New(t.JobType, t.Payload)
		j.TaskName = t.Name
		j.DependsOn = t.DependsOn
		id, err := s.Store.EnqueueTask(r.Context(), wfID, j)
		if err != nil {
			http.Error(w, "failed to enqueue task "+t.Name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
		taskIDs[t.Name] = id.String()
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"workflow_id": wfID.String(),
		"tasks":       taskIDs,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DB().PingContext(r.Context()); err != nil {
		http.Error(w, "db unreachable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
