package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"clockwork/internal/job"
	"clockwork/internal/leader"
	"clockwork/internal/store"
)

type CronSchedule struct {
	ID              uuid.UUID
	Name            string
	CronExpr        string
	JobType         string
	Payload         json.RawMessage
	LastScheduledAt *time.Time
}

type Scheduler struct {
	Store        *store.Store
	Elector      leader.Elector
	ReapInterval time.Duration
	CronInterval time.Duration
	cronParser   cron.Parser
}

func New(s *store.Store, e leader.Elector) *Scheduler {
	return &Scheduler{
		Store:        s,
		Elector:      e,
		ReapInterval: 10 * time.Second,
		CronInterval: 15 * time.Second,
		cronParser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	go s.reapLoop(ctx)

	for {
		if err := s.Elector.Campaign(ctx); err != nil {
			return err 
		}
		log.Println("scheduler: became leader, starting cron loop")
		s.cronLoopWhileLeader(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (s *Scheduler) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(s.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.Store.ReapExpiredLeases(ctx)
			if err != nil {
				log.Printf("reaper: error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("reaper: reclaimed %d jobs with expired leases", n)
			}
		}
	}
}

func (s *Scheduler) cronLoopWhileLeader(ctx context.Context) {
	ticker := time.NewTicker(s.CronInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.Elector.IsLeader() {
				return 
			}
			if err := s.evaluateCronSchedules(ctx); err != nil {
				log.Printf("scheduler: cron evaluation error: %v", err)
			}
		}
	}
}

func (s *Scheduler) evaluateCronSchedules(ctx context.Context) error {
	rows, err := s.Store.DB().QueryContext(ctx,
		`SELECT id, name, cron_expr, job_type, payload, last_scheduled_at
		 FROM cron_schedules WHERE enabled = true`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var schedules []CronSchedule
	for rows.Next() {
		var cs CronSchedule
		var last sql.NullTime
		if err := rows.Scan(&cs.ID, &cs.Name, &cs.CronExpr, &cs.JobType, &cs.Payload, &last); err != nil {
			return err
		}
		if last.Valid {
			cs.LastScheduledAt = &last.Time
		}
		schedules = append(schedules, cs)
	}

	now := time.Now().UTC()
	for _, cs := range schedules {
		sched, err := s.cronParser.Parse(cs.CronExpr)
		if err != nil {
			log.Printf("scheduler: bad cron expr for %s: %v", cs.Name, err)
			continue
		}
		from := now.Add(-s.CronInterval)
		if cs.LastScheduledAt != nil && cs.LastScheduledAt.After(from) {
			from = *cs.LastScheduledAt
		}
		next := sched.Next(from)
		if next.After(now) {
			continue 
		}

		idKey := cs.Name + "@" + next.Format(time.RFC3339)
		j := job.New(cs.JobType, cs.Payload, job.WithIdempotencyKey(idKey), job.WithRunAt(next))
		if _, err := s.Store.Enqueue(ctx, j); err != nil {
			log.Printf("scheduler: enqueue cron job %s failed: %v", cs.Name, err)
			continue
		}
		if _, err := s.Store.DB().ExecContext(ctx,
			`UPDATE cron_schedules SET last_scheduled_at=$1 WHERE id=$2`, next, cs.ID); err != nil {
			log.Printf("scheduler: update last_scheduled_at failed: %v", err)
		}
	}
	return nil
}