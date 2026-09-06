package queue
import (
	"context"
	"fmt"
	"log"
	"time"

	"clockwork/internal/backoff"
	"clockwork/internal/job"
	"clockwork/internal/store"
)

type Pool struct {
	Store         *store.Store
	Registry      *Registry
	NumWorkers    int
	PollInterval  time.Duration
	LeaseDuration time.Duration
	WorkerIDBase  string
}

func (p *Pool) Run(ctx context.Context) {
	done := make(chan struct{})
	for i := 0; i < p.NumWorkers; i++ {
		workerID := fmt.Sprintf("%s-%d", p.WorkerIDBase, i)
		go func(id string) {
			p.loop(ctx, id)
			done <- struct{}{}
		}(workerID)
	}
	for i := 0; i < p.NumWorkers; i++ {
		<-done
	}
}

func (p *Pool) loop(ctx context.Context, workerID string) {
	ticker := time.NewTicker(p.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] shutting down", workerID)
			return
		case <-ticker.C:
			j, err := p.Store.Claim(ctx, workerID, p.LeaseDuration)
			if err != nil {
				log.Printf("[%s] claim error: %v", workerID, err)
				continue
			}
			if j == nil {
				continue 
			}
			p.execute(ctx, workerID, j)
		}
	}
}

func (p *Pool) execute(ctx context.Context, workerID string, j *job.Job) {
	if err := p.Store.MarkRunning(ctx, j.ID, workerID); err != nil {
		log.Printf("[%s] mark running failed: %v", workerID, err)
		return
	}

	handler, ok := p.Registry.Get(j.JobType)
	if !ok {
		_ = p.Store.Fail(ctx, j.ID, workerID, fmt.Sprintf("no handler for job_type %q", j.JobType),
			backoff.Exponential(time.Second, time.Minute))
		return
	}

	hbCtx, cancelHB := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(p.LeaseDuration / 2)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := p.Store.Heartbeat(ctx, j.ID, workerID, p.LeaseDuration); err != nil {
					log.Printf("[%s] heartbeat lost for job %s: %v", workerID, j.ID, err)
					return
				}
			}
		}
	}()

	err := handler(ctx, j.Payload)
	cancelHB()

	if err != nil {
		log.Printf("[%s] job %s failed: %v", workerID, j.ID, err)
		if failErr := p.Store.Fail(ctx, j.ID, workerID, err.Error(),
			backoff.Exponential(time.Second, 2*time.Minute)); failErr != nil {
			log.Printf("[%s] recording failure also failed: %v", workerID, failErr)
		}
		return
	}
	if ackErr := p.Store.Ack(ctx, j.ID, workerID); ackErr != nil {
		log.Printf("[%s] ack failed for job %s: %v", workerID, j.ID, ackErr)
	}
}