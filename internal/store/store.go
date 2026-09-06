package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"clockwork/internal/job"
)

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Enqueue(ctx context.Context, j *job.Job) (uuid.UUID, error) {
	j.Status = job.StatusQueued

	const q = `
		INSERT INTO jobs (id, workflow_id, task_name, depends_on, job_type,
			payload, status, run_at, attempts, max_attempts, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`

	var returnedID uuid.UUID
	err := s.db.QueryRowContext(ctx, q,
		j.ID, j.WorkflowID, nullIfEmpty(j.TaskName), pq.Array(j.DependsOn),
		j.JobType, j.Payload, j.Status, j.RunAt, j.Attempts, j.MaxAttempts,
		j.IdempotencyKey,
	).Scan(&returnedID)

	if err == sql.ErrNoRows {
		var existingID uuid.UUID
		lookupErr := s.db.QueryRowContext(ctx,
			`SELECT id FROM jobs WHERE idempotency_key = $1`, j.IdempotencyKey,
		).Scan(&existingID)
		if lookupErr != nil {
			return uuid.Nil, fmt.Errorf("lookup deduped job: %w", lookupErr)
		}
		return existingID, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("enqueue: %w", err)
	}
	return returnedID, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, leaseDuration time.Duration) (*job.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() 

	const selectQ = `
		SELECT j.id, j.workflow_id, j.task_name, j.depends_on, j.job_type,
		       j.payload, j.status, j.run_at, j.attempts, j.max_attempts,
		       j.idempotency_key, j.created_at, j.updated_at
		FROM jobs j
		WHERE j.status = 'QUEUED'
		  AND j.run_at <= now()
		  -- DAG dependency gate: a task is only claimable once every task
		  -- it depends_on (within the same workflow) has SUCCEEDED.
		  AND NOT EXISTS (
		      SELECT 1 FROM unnest(j.depends_on) dep
		      WHERE NOT EXISTS (
		          SELECT 1 FROM jobs d
		          WHERE d.workflow_id = j.workflow_id
		            AND d.task_name = dep
		            AND d.status = 'SUCCESS'
		      )
		  )
		ORDER BY j.run_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`

	var j job.Job
	var workflowID sql.NullString
	var taskName sql.NullString
	var idKey sql.NullString

	row := tx.QueryRowContext(ctx, selectQ)
	err = row.Scan(&j.ID, &workflowID, &taskName, pq.Array(&j.DependsOn),
		&j.JobType, &j.Payload, &j.Status, &j.RunAt, &j.Attempts, &j.MaxAttempts,
		&idKey, &j.CreatedAt, &j.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil 
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}
	if workflowID.Valid {
		id := uuid.MustParse(workflowID.String)
		j.WorkflowID = &id
	}
	if taskName.Valid {
		j.TaskName = taskName.String
	}
	if idKey.Valid {
		j.IdempotencyKey = &idKey.String
	}

	leaseExpiry := time.Now().UTC().Add(leaseDuration)
	const updateQ = `
		UPDATE jobs SET status = 'LEASED', locked_by = $1,
			lease_expires_at = $2, updated_at = now()
		WHERE id = $3`
	if _, err := tx.ExecContext(ctx, updateQ, workerID, leaseExpiry, j.ID); err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}

	j.Status = job.StatusLeased
	j.LockedBy = &workerID
	j.LeaseExpiresAt = &leaseExpiry
	return &j, nil
}

func (s *Store) Heartbeat(ctx context.Context, jobID uuid.UUID, workerID string, leaseDuration time.Duration) error {
	newExpiry := time.Now().UTC().Add(leaseDuration)
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET lease_expires_at = $1, updated_at = now()
		 WHERE id = $2 AND locked_by = $3 AND status IN ('LEASED','RUNNING')`,
		newExpiry, jobID, workerID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("heartbeat: job %s no longer owned by %s (lease likely reaped)", jobID, workerID)
	}
	return nil
}

func (s *Store) MarkRunning(ctx context.Context, jobID uuid.UUID, workerID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status='RUNNING', updated_at=now()
		 WHERE id=$1 AND locked_by=$2 AND status='LEASED'`, jobID, workerID)
	return err
}

func (s *Store) Ack(ctx context.Context, jobID uuid.UUID, workerID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status='SUCCESS', updated_at=now()
		 WHERE id=$1 AND locked_by=$2 AND status='RUNNING'`, jobID, workerID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("ack: job %s not owned by %s in RUNNING state", jobID, workerID)
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, jobID uuid.UUID, workerID string, errMsg string, backoff func(int) time.Duration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx,
		`SELECT attempts, max_attempts FROM jobs WHERE id=$1 AND locked_by=$2 FOR UPDATE`,
		jobID, workerID).Scan(&attempts, &maxAttempts)
	if err != nil {
		return fmt.Errorf("fail lookup: %w", err)
	}

	attempts++
	if attempts >= maxAttempts {
		_, err = tx.ExecContext(ctx,
			`UPDATE jobs SET status='DEAD', attempts=$1, last_error=$2,
			 locked_by=NULL, lease_expires_at=NULL, updated_at=now() WHERE id=$3`,
			attempts, errMsg, jobID)
	} else {
		nextRun := time.Now().UTC().Add(backoff(attempts))
		_, err = tx.ExecContext(ctx,
			`UPDATE jobs SET status='QUEUED', attempts=$1, last_error=$2,
			 run_at=$3, locked_by=NULL, lease_expires_at=NULL, updated_at=now() WHERE id=$4`,
			attempts, errMsg, nextRun, jobID)
	}
	if err != nil {
		return fmt.Errorf("fail update: %w", err)
	}
	return tx.Commit()
}

func (s *Store) ReapExpiredLeases(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'QUEUED', locked_by = NULL, lease_expires_at = NULL,
		    attempts = attempts + 1, updated_at = now(),
		    last_error = 'lease expired: worker presumed dead'
		WHERE status IN ('LEASED', 'RUNNING')
		  AND lease_expires_at < now()`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) CreateWorkflow(ctx context.Context, name string) (uuid.UUID, error) {
	id := uuid.New()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workflows (id, name, status) VALUES ($1,$2,'RUNNING')`, id, name)
	if err != nil {
		return uuid.Nil, err
	}
	s.appendEvent(ctx, id, "WorkflowStarted", "", nil)
	return id, nil
}

func (s *Store) EnqueueTask(ctx context.Context, workflowID uuid.UUID, j *job.Job) (uuid.UUID, error) {
	j.WorkflowID = &workflowID
	id, err := s.Enqueue(ctx, j)
	if err == nil {
		s.appendEvent(ctx, workflowID, "TaskScheduled", j.TaskName, j.Payload)
	}
	return id, err
}

func (s *Store) appendEvent(ctx context.Context, workflowID uuid.UUID, eventType, taskName string, data json.RawMessage) {
	if data == nil {
		data = json.RawMessage(`{}`)
	}
	_, _ = s.db.ExecContext(ctx,
		`INSERT INTO workflow_events (workflow_id, event_type, task_name, data) VALUES ($1,$2,$3,$4)`,
		workflowID, eventType, nullIfEmpty(taskName), data)
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
