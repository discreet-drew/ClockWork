package leader

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type Elector interface {
	Campaign(ctx context.Context) error
	IsLeader() bool
	Resign(ctx context.Context) error
}

type PGElector struct {
	db       *sql.DB
	lockKey  int64
	conn     *sql.Conn
	isLeader bool
}

func NewPGElector(db *sql.DB, lockKey int64) *PGElector {
	return &PGElector{db: db, lockKey: lockKey}
}

func (e *PGElector) Campaign(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := e.db.Conn(ctx)
		if err != nil {
			log.Printf("leader: failed to get connection: %v", err)
			<-ticker.C
			continue
		}

		var acquired bool
		err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, e.lockKey).Scan(&acquired)
		if err != nil {
			conn.Close()
			log.Printf("leader: lock attempt failed: %v", err)
			<-ticker.C
			continue
		}
		if acquired {
			e.conn = conn
			e.isLeader = true
			log.Println("leader: acquired leadership")
			return nil
		}
		conn.Close()
		<-ticker.C
	}
}

func (e *PGElector) IsLeader() bool { return e.isLeader }

func (e *PGElector) Resign(ctx context.Context) error {
	if e.conn == nil {
		return nil
	}
	_, err := e.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, e.lockKey)
	closeErr := e.conn.Close()
	e.isLeader = false
	e.conn = nil
	if err != nil {
		return err
	}
	return closeErr
}