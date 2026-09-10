# ClockWork

ClockWork is a distributed job scheduler and workflow engine built in Go and backed by PostgreSQL.
It allows applications to submit jobs, schedule recurring tasks, execute work using a pool of workers, retry failed jobs, recover jobs from worker failures, and manage tasks with dependencies.

## Features

- **Job Scheduling** : Submit and schedule background jobs through an HTTP API.
- **Durable Jobs** : Jobs are stored in PostgreSQL so they survive process and worker restarts.
- **Worker Pool** : Multiple workers can safely process jobs concurrently.
- **Job Leases** : Workers temporarily lease jobs while executing them.
- **Crash Recovery** : Expired leases allow unfinished jobs to be picked up by another worker.
- **Retries** : Failed jobs are retried using exponential backoff up to a configured limit.
- **DAG Workflows** : Tasks can depend on other tasks and are only executed when their dependencies succeed.
- **Cron Scheduling** : Recurring jobs can be triggered using cron expressions.
- **Leader Election** : PostgreSQL advisory locks prevent multiple scheduler replicas from processing the same schedule simultaneously.
- **Idempotency** : Idempotency keys prevent duplicate scheduled jobs.

>PostgreSQL acts as the central source of truth for job state.
>Workers use _SELECT ... FOR UPDATE SKIP LOCKED_ to safely claim available jobs. Each claimed job receives a lease that is periodically extended through heartbeats. If the worker fails and the lease expires, the job can be reclaimed by another worker.
>Workflow dependencies are evaluated during job claiming, allowing the same queue mechanism to control both job execution and DAG execution order.


## Architecture

<img width="374" height="470" alt="image" src="https://github.com/user-attachments/assets/f6594865-3c82-4142-b461-3838182db2a4" />


## Project Structure
ClockWork/ \
│ \
├── cmd/ \
│   ├── api/\
│   ├── scheduler/ \
│   └── worker/ \
│ \
├── internal/ \
│   ├── job/ \
│   ├── store/ \
│   ├── queue/ \
│   ├── leader/ \
│   └── backoff/ \
│ \
├── migrations/  \
├── scripts/ \
├── Dockerfile \
├── docker-compose.yml \
└── go.mod 

## Running ClockWork
Requirements 
- Go 1.22+ 
- Docker Desktop 
- PostgreSQL 

Run with Docker 
`docker compose up --build` 

This starts PostgreSQL, the API, workers, and scheduler instances. 

Run manually 

Start PostgreSQL: 

`docker run --name cw-postgres \` \
 ` -e POSTGRES_PASSWORD=devpass \` \
  `-e POSTGRES_DB=Clockwork \` \
  `-p 5432:5432 \` \
  `-d postgres:16` 

Run the database migration: 

`psql "postgres://postgres:devpass@localhost:5432/Clockwork?sslmode=disable" \` \
  `-f migrations/001_schema.sql`

Set the database URL: 

`export DATABASE_URL="postgres://postgres:devpass@localhost:5432/Clockwork?sslmode=disable"`

Start the services: 

`go run ./cmd/api` \
`go run ./cmd/worker` \
`go run ./cmd/scheduler` 

## Example

Submit a job:

`curl -X POST http://localhost:8080/jobs \` \
  `-d '{"job_type":"print","payload":{"message":"hello"}}' `

Check the job:

`curl http://localhost:8080/jobs/<id>`

Run the complete smoke test:

`./scripts/smoke_test.sh`

## Testing

Run unit tests:

`go test ./internal/job/... ./internal/backoff/...`

Run database-related tests:

`go test ./internal/store/... -v`

## What I Learned

Building ClockWork gave me practical experience with several backend and distributed-systems concepts:

* Designing a durable job queue using PostgreSQL.
* Managing concurrent workers with database row locking.
* Understanding FOR UPDATE SKIP LOCKED and how it enables safe job claiming.
* Implementing leases and heartbeats for temporary job ownership.
* Designing retry systems using exponential backoff.
* Handling worker failures and crash recovery.
* Building DAG-based task dependencies on top of a job queue.
* Using PostgreSQL advisory locks for leader election.
* Using idempotency keys to prevent duplicate scheduled jobs.
* Separating scheduling, job execution, persistence, and API responsibilities.
* Thinking about failure modes and recovery rather than only the successful execution path.

Current Scope

> ClockWork is primarily a backend and distributed-systems learning project.

Current limitations include:

* No web UI
* No API authentication
* No metrics dashboard
* No Kubernetes deployment

The execution model uses at-least-once delivery with idempotency mechanisms rather than claiming strict exactly-once execution.
