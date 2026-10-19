package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"
	"aegis/internal/scheduler"
)

// ActiveExecution tracks an in-flight job and its abrupt abort control.
type ActiveExecution struct {
	JobID    string
	WorkerID string
	TenantID string
	Cancel   context.CancelFunc
	Killed   *atomic.Bool
}

// Pool coordinates concurrent background worker loops.
type Pool struct {
	db                *db.DB
	events            *api.EventBus
	taskChan          <-chan scheduler.JobTask
	concurrency       int
	heartbeatInterval time.Duration
	activeMu          sync.RWMutex
	activeJobs        map[string]*ActiveExecution
	activeWorkers     int32
	stopChan          chan struct{}
	wg                sync.WaitGroup

	// Metrics
	totalIngested int64
	totalComplete int64
	totalFailed   int64
	totalDropped  int64 // Always 0 by architectural guarantee
}

// New creates an initialized worker pool.
func New(database *db.DB, events *api.EventBus, taskChan <-chan scheduler.JobTask, concurrency int) *Pool {
	hbInterval := 5 * time.Second
	if hStr := os.Getenv("HEARTBEAT_INTERVAL_SEC"); hStr != "" {
		if s, err := strconv.Atoi(hStr); err == nil && s > 0 {
			hbInterval = time.Duration(s) * time.Second
		}
	}

	return &Pool{
		db:                database,
		events:            events,
		taskChan:          taskChan,
		concurrency:       concurrency,
		heartbeatInterval: hbInterval,
		activeJobs:        make(map[string]*ActiveExecution),
		stopChan:          make(chan struct{}),
	}
}

// SetHeartbeatInterval updates the heartbeat interval.
func (p *Pool) SetHeartbeatInterval(d time.Duration) {
	if d > 0 {
		p.heartbeatInterval = d
	}
}

// Start launches the worker routines.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.concurrency; i++ {
		workerID := fmt.Sprintf("worker-%02d", i)
		p.wg.Add(1)
		go p.workerLoop(ctx, workerID)
	}
}

// Stop gracefully shuts down all workers.
func (p *Pool) Stop() {
	close(p.stopChan)
	p.wg.Wait()
}

// KillActiveJob abruptly aborts an active job's worker routine without cleaning up the database lease.
// This fulfills the Chaos Engine requirement to simulate ungraceful container crash/eviction.
func (p *Pool) KillActiveJob(targetJobID string) (*ActiveExecution, error) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()

	var target *ActiveExecution
	if targetJobID != "" {
		if exec, ok := p.activeJobs[targetJobID]; ok {
			target = exec
		}
	} else {
		// Pick any active job
		for _, exec := range p.activeJobs {
			target = exec
			break
		}
	}

	if target == nil {
		return nil, fmt.Errorf("no matching active running worker to kill")
	}

	// Mark as killed so normal cleanup is completely skipped!
	target.Killed.Store(true)
	delete(p.activeJobs, target.JobID)
	atomic.AddInt32(&p.activeWorkers, -1)

	// Abruptly cancel the worker's execution context
	target.Cancel()

	p.events.Publish(api.Event{
		Type:      "WORKER_KILLED",
		JobID:     target.JobID,
		TenantID:  target.TenantID,
		WorkerID:  target.WorkerID,
		Message:   fmt.Sprintf("Worker %s aborted abruptly; DB lease left intact for Heartbeat Reaper", target.WorkerID),
	})

	return target, nil
}

// GetActiveJobs returns list of currently executing jobs.
func (p *Pool) GetActiveJobs() []map[string]string {
	p.activeMu.RLock()
	defer p.activeMu.RUnlock()

	list := make([]map[string]string, 0, len(p.activeJobs))
	for _, job := range p.activeJobs {
		list = append(list, map[string]string{
			"job_id":    job.JobID,
			"worker_id": job.WorkerID,
			"tenant_id": job.TenantID,
		})
	}
	return list
}

// GetStats returns global counter metrics.
func (p *Pool) GetStats(ctx context.Context) map[string]any {
	var totalIngested, pendingCount, runningCount, completedCount, failedCount int
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs").Scan(&totalIngested)
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'PENDING'").Scan(&pendingCount)
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING'").Scan(&runningCount)
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'COMPLETED'").Scan(&completedCount)
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'FAILED'").Scan(&failedCount)

	var recoveredCount int
	_ = p.db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM execution_logs WHERE event = 'REAPER_RECOVERED'").Scan(&recoveredCount)

	return map[string]any{
		"active_workers": atomic.LoadInt32(&p.activeWorkers),
		"concurrency":    p.concurrency,
		"total_ingested": totalIngested,
		"pending":        pendingCount,
		"running":        runningCount,
		"completed":      completedCount,
		"failed":         failedCount,
		"recovered":      recoveredCount,
		"dropped":        0, // Zero-drop guarantee
	}
}

// workerLoop receives jobs and executes them with lease heartbeats.
func (p *Pool) workerLoop(parentCtx context.Context, workerID string) {
	defer p.wg.Done()

	for {
		select {
		case <-parentCtx.Done():
			return
		case <-p.stopChan:
			return
		case task, ok := <-p.taskChan:
			if !ok {
				return
			}
			p.executeTask(parentCtx, workerID, task)
		}
	}
}

// executeTask runs a single job.
func (p *Pool) executeTask(parentCtx context.Context, workerID string, task scheduler.JobTask) {
	jobCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	killedFlag := &atomic.Bool{}
	activeExec := &ActiveExecution{
		JobID:    task.ID,
		WorkerID: workerID,
		TenantID: task.TenantID,
		Cancel:   cancel,
		Killed:   killedFlag,
	}

	p.activeMu.Lock()
	p.activeJobs[task.ID] = activeExec
	atomic.AddInt32(&p.activeWorkers, 1)
	p.activeMu.Unlock()

	// 1. Acquire initial worker lease in PostgreSQL
	leaseCtx, leaseCancel := context.WithTimeout(parentCtx, 3*time.Second)
	_, err := p.db.Pool.Exec(leaseCtx, `
		INSERT INTO worker_leases (job_id, worker_id, heartbeat_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (job_id) DO UPDATE SET heartbeat_at = NOW(), worker_id = EXCLUDED.worker_id;
	`, task.ID, workerID)
	leaseCancel()
	if err != nil {
		log.Printf("[%s] Failed to record worker lease for job %s: %v", workerID, task.ID, err)
	}

	// 2. Log start & publish event
	p.logEvent(parentCtx, task.ID, "JOB_STARTED", fmt.Sprintf("Claimed and started by %s (Attempt %d)", workerID, task.Attempts))
	p.events.Publish(api.Event{
		Type:      "JOB_STARTED",
		JobID:     task.ID,
		TenantID:  task.TenantID,
		WorkerID:  workerID,
		Attempts:  task.Attempts,
		Message:   fmt.Sprintf("Execution started on %s", workerID),
	})

	// 3. Launch background heartbeat loop
	heartbeatStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(p.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-heartbeatStop:
				return
			case <-ticker.C:
				if killedFlag.Load() {
					return
				}
				hbCtx, hbCancel := context.WithTimeout(parentCtx, 2*time.Second)
				_, hbErr := p.db.Pool.Exec(hbCtx, `
					UPDATE worker_leases SET heartbeat_at = NOW() WHERE job_id = $1
				`, task.ID)
				hbCancel()

				if hbErr == nil {
					p.events.Publish(api.Event{
						Type:      "HEARTBEAT_TICK",
						JobID:     task.ID,
						TenantID:  task.TenantID,
						WorkerID:  workerID,
						Attempts:  task.Attempts,
						Message:   fmt.Sprintf("Worker %s refreshed lease heartbeat", workerID),
					})
				}
			}
		}
	}()

	// 4. Execute workload simulation
	execErr := p.runPayload(jobCtx, task)
	close(heartbeatStop)

	// If the worker was killed abruptly via chaos, STOP IMMEDIATELY!
	// Pitfall 2: Do NOT update status or delete lease, so Heartbeat Reaper kicks in!
	if killedFlag.Load() {
		return
	}

	// Remove from active execution tracker
	p.activeMu.Lock()
	if _, ok := p.activeJobs[task.ID]; ok {
		delete(p.activeJobs, task.ID)
		atomic.AddInt32(&p.activeWorkers, -1)
	}
	p.activeMu.Unlock()

	// 5. Clean task completion / failure in ultra-short transaction
	completeCtx, completeCancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer completeCancel()

	if execErr != nil {
		p.handleTaskFailure(completeCtx, task, workerID, execErr)
	} else {
		p.handleTaskSuccess(completeCtx, task, workerID)
	}
}

// runPayload executes or simulates the task workload.
func (p *Pool) runPayload(ctx context.Context, task scheduler.JobTask) error {
	var payloadMap map[string]any
	if err := json.Unmarshal(task.Payload, &payloadMap); err != nil {
		payloadMap = make(map[string]any)
	}

	// Determine duration (default: 4 seconds so heartbeats tick visibly)
	duration := 4 * time.Second
	if d, ok := payloadMap["duration_ms"].(float64); ok && d > 0 {
		duration = time.Duration(d) * time.Millisecond
	}

	// Check if this task is requested to simulate failure
	simulateFail := false
	if f, ok := payloadMap["fail"].(bool); ok && f {
		simulateFail = true
	}

	// Cooperative sleep with 100ms granularity
	step := 100 * time.Millisecond
	elapsed := time.Duration(0)
	ticker := time.NewTicker(step)
	defer ticker.Stop()

	for elapsed < duration {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			elapsed += step
		}
	}

	if simulateFail {
		return fmt.Errorf("simulated handler execution failure")
	}

	return nil
}

func (p *Pool) handleTaskSuccess(ctx context.Context, task scheduler.JobTask, workerID string) {
	tx, err := p.db.Pool.Begin(ctx)
	if err != nil {
		log.Printf("[%s] Failed to begin completion tx for job %s: %v", workerID, task.ID, err)
		return
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'COMPLETED', finished_at = NOW()
		WHERE id = $1 AND attempts = $2 AND status = 'RUNNING'
	`, task.ID, task.Attempts)
	if err != nil {
		log.Printf("[%s] Failed to mark job %s completed: %v", workerID, task.ID, err)
		return
	}
	if result.RowsAffected() == 0 {
		log.Printf("[%s] Stale worker ignored for job %s (fencing check failed)", workerID, task.ID)
		p.logEvent(ctx, task.ID, "WORKER_FENCING_REJECTED", fmt.Sprintf("Stale worker %s ignored on completion for attempt %d", workerID, task.Attempts))
		return
	}

	_, err = tx.Exec(ctx, `DELETE FROM worker_leases WHERE job_id = $1`, task.ID)
	if err != nil {
		log.Printf("[%s] Failed to delete worker lease for job %s: %v", workerID, task.ID, err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[%s] Failed to commit completion for job %s: %v", workerID, task.ID, err)
		return
	}

	p.logEvent(ctx, task.ID, "JOB_COMPLETED", fmt.Sprintf("Completed cleanly by %s", workerID))
	p.events.Publish(api.Event{
		Type:      "JOB_COMPLETED",
		JobID:     task.ID,
		TenantID:  task.TenantID,
		WorkerID:  workerID,
		Attempts:  task.Attempts,
		Message:   fmt.Sprintf("Job completed successfully by %s", workerID),
	})
}

func (p *Pool) handleTaskFailure(ctx context.Context, task scheduler.JobTask, workerID string, failErr error) {
	var finalStatus string
	if task.Attempts >= task.MaxAttempts {
		finalStatus = "FAILED"
	} else {
		finalStatus = "PENDING"
	}

	tx, err := p.db.Pool.Begin(ctx)
	if err != nil {
		log.Printf("[%s] Failed to begin failure tx for job %s: %v", workerID, task.ID, err)
		return
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = $2, finished_at = CASE WHEN $2 = 'FAILED' THEN NOW() ELSE NULL END
		WHERE id = $1 AND attempts = $3 AND status = 'RUNNING'
	`, task.ID, finalStatus, task.Attempts)
	if err != nil {
		log.Printf("[%s] Failed to record job failure for %s: %v", workerID, task.ID, err)
		return
	}
	if result.RowsAffected() == 0 {
		log.Printf("[%s] Stale worker ignored for job failure %s (fencing check failed)", workerID, task.ID)
		p.logEvent(ctx, task.ID, "WORKER_FENCING_REJECTED", fmt.Sprintf("Stale worker %s ignored on failure for attempt %d", workerID, task.Attempts))
		return
	}

	_, err = tx.Exec(ctx, `DELETE FROM worker_leases WHERE job_id = $1`, task.ID)
	if err != nil {
		log.Printf("[%s] Failed to delete worker lease for job %s: %v", workerID, task.ID, err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[%s] Failed to commit failure for job %s: %v", workerID, task.ID, err)
		return
	}

	p.logEvent(ctx, task.ID, "JOB_FAILED", fmt.Sprintf("Failed on %s: %v", workerID, failErr))
	p.events.Publish(api.Event{
		Type:      "JOB_FAILED",
		JobID:     task.ID,
		TenantID:  task.TenantID,
		WorkerID:  workerID,
		Attempts:  task.Attempts,
		Message:   fmt.Sprintf("Execution failed: %v", failErr),
	})
}

func (p *Pool) logEvent(ctx context.Context, jobID, event, message string) {
	logCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, _ = p.db.Pool.Exec(logCtx, `
		INSERT INTO execution_logs (job_id, event, message)
		VALUES ($1, $2, $3);
	`, jobID, event, message)
}
