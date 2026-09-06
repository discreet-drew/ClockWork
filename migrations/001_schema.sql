CREATE TABLE IF NOT EXISTS workflows (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'RUNNING', 
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY,
    workflow_id       UUID REFERENCES workflows(id) ON DELETE CASCADE,
    task_name         TEXT,               
    depends_on        TEXT[] NOT NULL DEFAULT '{}',
    job_type          TEXT NOT NULL,      
    payload           JSONB NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL DEFAULT 'CREATED',
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts          INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    idempotency_key   TEXT UNIQUE,
    locked_by         TEXT,               
    lease_expires_at  TIMESTAMPTZ,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (run_at)
    WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry
    ON jobs (lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_jobs_workflow ON jobs (workflow_id);

CREATE TABLE IF NOT EXISTS cron_schedules (
    id                UUID PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    cron_expr         TEXT NOT NULL,
    job_type          TEXT NOT NULL,
    payload           JSONB NOT NULL DEFAULT '{}',
    last_scheduled_at TIMESTAMPTZ,
    enabled           BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_events (
    id           BIGSERIAL PRIMARY KEY,
    workflow_id  UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    event_type   TEXT NOT NULL, 
    task_name    TEXT,
    data         JSONB NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_wf ON workflow_events (workflow_id, id);