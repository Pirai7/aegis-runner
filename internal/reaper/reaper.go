package reaper

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"
)

// Reaper runs a periodic background daemon to detect stale leases and rescue orphaned tasks.
type Reaper struct {
	db       *db.DB
	events   *api.EventBus
	interval time.Duration
	ttl      time.Duration
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// New creates a new Heartbeat Reaper daemon.
func New(database *db.DB, events *api.EventBus, interval, ttl time.Duration) *Reaper {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	return &Reaper{
		db:       database,
		events:   events,
		interval: interval,
		ttl:      ttl,
		stopChan: make(chan struct{}),
	}
}

// Start launches the background reaper sweep loop.
func (r *Reaper) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopChan:
				return
			case <-ticker.C:
				r.sweep(ctx)
			}
		}
	}()
}

// Stop halts the reaper loop.
func (r *Reaper) Stop() {
	close(r.stopChan)
	r.wg.Wait()
}

// sweep executes the atomic orphan detection and recovery query.
func (r *Reaper) sweep(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Query to find expired leases and atomically update jobs
	query := `
		WITH expired_leases AS (
			SELECT job_id, worker_id, heartbeat_at
			FROM worker_leases
			WHERE heartbeat_at < NOW() - ($1 * INTERVAL '1 second')
		),
		reclaimed_jobs AS (
			UPDATE jobs j
			SET status = CASE WHEN j.attempts >= j.max_attempts THEN 'FAILED' ELSE 'PENDING' END
			FROM expired_leases el
			WHERE j.id = el.job_id
			RETURNING j.id, j.tenant_id, j.attempts, j.max_attempts, j.status, el.worker_id
		),
		deleted_leases AS (
			DELETE FROM worker_leases
			WHERE job_id IN (SELECT job_id FROM expired_leases)
		)
		SELECT id, tenant_id, attempts, max_attempts, status, worker_id FROM reclaimed_jobs;
	`

	rows, err := r.db.Pool.Query(sweepCtx, query, int(r.ttl.Seconds()))
	if err != nil {
		log.Printf("[REAPER] Error executing sweep query: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var jobID, tenantID, status, workerID string
		var attempts, maxAttempts int

		if err := rows.Scan(&jobID, &tenantID, &attempts, &maxAttempts, &status, &workerID); err != nil {
			log.Printf("[REAPER] Error scanning reclaimed job: %v", err)
			continue
		}

		message := fmt.Sprintf("Worker %s heartbeat expired beyond %ds TTL threshold; job rescued to %s (attempts: %d/%d)",
			workerID, int(r.ttl.Seconds()), status, attempts, maxAttempts)

		// Record in execution logs
		_, _ = r.db.Pool.Exec(sweepCtx, `
			INSERT INTO execution_logs (job_id, event, message)
			VALUES ($1, 'REAPER_RECOVERED', $2)
		`, jobID, message)

		// Broadcast recovery event
		r.events.Publish(api.Event{
			Type:      "REAPER_RECOVERED",
			JobID:     jobID,
			TenantID:  tenantID,
			WorkerID:  workerID,
			Attempts:  attempts,
			Message:   message,
			Details: map[string]any{
				"new_status":   status,
				"max_attempts": maxAttempts,
			},
		})

		log.Printf("[REAPER] %s", message)
	}
}
