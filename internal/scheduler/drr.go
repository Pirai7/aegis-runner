package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"

	"github.com/jackc/pgx/v5"
)

// Tenant represents an active tenant in the DRR scheduler.
type Tenant struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Quantum          int    `json:"quantum"`
	Deficit          int    `json:"deficit"`
	ConcurrencyLimit int    `json:"concurrency_limit"`
	ActiveRunning    int    `json:"active_running"`
	PendingCount     int    `json:"pending_count"`
	CompletedCount   int    `json:"completed_count"`
}

// JobTask is a dequeued job ready for worker execution.
type JobTask struct {
	ID          string
	TenantID    string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
}

// DRRScheduler schedules jobs fairly across tenants using Deficit Round-Robin.
type DRRScheduler struct {
	db          *db.DB
	events      *api.EventBus
	mu          sync.RWMutex
	tenants     []*Tenant
	tenantIndex int
	taskChan    chan<- JobTask
	costPerJob  int
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// New creates a new Deficit Round-Robin scheduler.
func New(database *db.DB, events *api.EventBus, taskChan chan<- JobTask) *DRRScheduler {
	return &DRRScheduler{
		db:         database,
		events:     events,
		tenants:    make([]*Tenant, 0),
		taskChan:   taskChan,
		costPerJob: 10,
		stopChan:   make(chan struct{}),
	}
}

// RefreshTenants syncs tenant configurations from the database.
func (s *DRRScheduler) RefreshTenants(ctx context.Context) error {
	rows, err := s.db.Pool.Query(ctx, `
		SELECT t.id, t.name, t.quantum, t.concurrency_limit,
		       COALESCE((SELECT COUNT(*) FROM jobs WHERE tenant_id = t.id AND status = 'RUNNING'), 0) AS active_running,
		       COALESCE((SELECT COUNT(*) FROM jobs WHERE tenant_id = t.id AND status = 'PENDING'), 0) AS pending_count,
		       COALESCE((SELECT COUNT(*) FROM jobs WHERE tenant_id = t.id AND status = 'COMPLETED'), 0) AS completed_count
		FROM tenants t
		ORDER BY t.created_at ASC
	`)
	if err != nil {
		return fmt.Errorf("failed to query tenants: %w", err)
	}
	defer rows.Close()

	var updated []*Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.Quantum, &t.ConcurrencyLimit, &t.ActiveRunning, &t.PendingCount, &t.CompletedCount); err != nil {
			return err
		}
		updated = append(updated, &t)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Preserve existing deficit counters if tenant exists
	existingMap := make(map[string]int)
	for _, t := range s.tenants {
		existingMap[t.ID] = t.Deficit
	}

	for _, t := range updated {
		if d, ok := existingMap[t.ID]; ok {
			t.Deficit = d
		} else {
			t.Deficit = t.Quantum
		}
	}

	s.tenants = updated
	return nil
}

// GetTenantMetrics returns a snapshot of tenant statistics for dashboard display.
func (s *DRRScheduler) GetTenantMetrics(ctx context.Context) ([]api.TenantMetric, error) {
	if err := s.RefreshTenants(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]api.TenantMetric, len(s.tenants))
	for i, t := range s.tenants {
		result[i] = api.TenantMetric{
			ID:               t.ID,
			Name:             t.Name,
			Quantum:          t.Quantum,
			Deficit:          t.Deficit,
			ConcurrencyLimit: t.ConcurrencyLimit,
			ActiveRunning:    t.ActiveRunning,
			PendingCount:     t.PendingCount,
			CompletedCount:   t.CompletedCount,
		}
	}
	return result, nil
}

// Start runs the DRR scheduling loop.
func (s *DRRScheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Initial tenant load
		_ = s.RefreshTenants(ctx)

		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()

		tenantSyncTick := time.NewTicker(3 * time.Second)
		defer tenantSyncTick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			case <-tenantSyncTick.C:
				_ = s.RefreshTenants(ctx)
			case <-tick.C:
				s.scheduleRound(ctx)
			}
		}
	}()
}

// Stop gracefully stops the scheduler.
func (s *DRRScheduler) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

// scheduleRound executes one scheduling tick across tenants.
func (s *DRRScheduler) scheduleRound(ctx context.Context) {
	s.mu.Lock()
	if len(s.tenants) == 0 {
		s.mu.Unlock()
		return
	}

	// Work with the current tenant in the round
	if s.tenantIndex >= len(s.tenants) {
		s.tenantIndex = 0
	}
	t := s.tenants[s.tenantIndex]
	s.tenantIndex = (s.tenantIndex + 1) % len(s.tenants)

	// Increment quantum
	t.Deficit += t.Quantum
	if t.Deficit > t.Quantum*5 { // Cap maximum deficit accumulation
		t.Deficit = t.Quantum * 5
	}
	tenantID := t.ID
	limit := t.ConcurrencyLimit
	deficit := t.Deficit
	s.mu.Unlock()

	// Check running jobs count for this tenant
	var runningCount int
	err := s.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE tenant_id = $1 AND status = 'RUNNING'", tenantID).Scan(&runningCount)
	if err != nil {
		return
	}

	if runningCount >= limit {
		return
	}

	// Dequeue up to allowed deficit and concurrency
	for deficit >= s.costPerJob && runningCount < limit {
		task, err := s.dequeueJobForTenant(ctx, tenantID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No pending jobs for this tenant; reset deficit so it doesn't hoard credits
				s.mu.Lock()
				t.Deficit = 0
				s.mu.Unlock()
				break
			}
			return
		}

		// Job dequeued successfully!
		s.mu.Lock()
		t.Deficit -= s.costPerJob
		deficit = t.Deficit
		s.mu.Unlock()

		runningCount++

		s.events.Publish(api.Event{
			Type:      "JOB_SCHEDULED",
			JobID:     task.ID,
			TenantID:  task.TenantID,
			Attempts:  task.Attempts,
			Message:   fmt.Sprintf("Job scheduled via DRR for tenant %s (deficit remaining: %d)", task.TenantID, deficit),
		})

		select {
		case s.taskChan <- *task:
		case <-ctx.Done():
			return
		}
	}
}

// dequeueJobForTenant atomically claims a pending job for a specific tenant using SKIP LOCKED.
func (s *DRRScheduler) dequeueJobForTenant(ctx context.Context, tenantID string) (*JobTask, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
		WITH next_job AS (
			SELECT id FROM jobs
			WHERE tenant_id = $1 AND status = 'PENDING' AND scheduled_for <= NOW()
			ORDER BY scheduled_for ASC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET status = 'RUNNING', started_at = NOW(), attempts = attempts + 1
		FROM next_job
		WHERE j.id = next_job.id
		RETURNING j.id, j.tenant_id, j.payload, j.attempts, j.max_attempts;
	`

	var task JobTask
	err := s.db.Pool.QueryRow(queryCtx, query, tenantID).Scan(
		&task.ID,
		&task.TenantID,
		&task.Payload,
		&task.Attempts,
		&task.MaxAttempts,
	)
	if err != nil {
		return nil, err
	}

	return &task, nil
}
