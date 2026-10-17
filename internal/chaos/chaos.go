package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"
	"aegis/internal/worker"
)

// Engine handles chaos engineering actions for live resilience testing.
type Engine struct {
	db          *db.DB
	events      *api.EventBus
	pool        *worker.Pool
	simLatency  int64 // Milliseconds of artificial latency to inject if > 0
}

// New creates an initialized Chaos Engine.
func New(database *db.DB, events *api.EventBus, pool *worker.Pool) *Engine {
	return &Engine{
		db:     database,
		events: events,
		pool:   pool,
	}
}

// KillWorkerRequest payload.
type KillWorkerRequest struct {
	JobID string `json:"job_id"`
}

// HandleKillWorker processes POST /api/v1/chaos/kill-worker.
func (e *Engine) HandleKillWorker(w http.ResponseWriter, r *http.Request) {
	var req KillWorkerRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	exec, err := e.pool.KillActiveJob(req.JobID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "aborted",
		"message":   "Worker abruptly aborted without cleaning DB lease",
		"job_id":    exec.JobID,
		"worker_id": exec.WorkerID,
		"tenant_id": exec.TenantID,
	})
}

// FloodRequest payload.
type FloodRequest struct {
	TenantID string `json:"tenant_id"`
	Count    int    `json:"count"`
}

// HandleFlood processes POST /api/v1/chaos/flood.
func (e *Engine) HandleFlood(w http.ResponseWriter, r *http.Request) {
	var req FloodRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	if req.TenantID == "" {
		req.TenantID = "tenant-alpha"
	}
	if req.Count <= 0 || req.Count > 500 {
		req.Count = 50
	}

	// Launch background flood insertion
	go func(tenantID string, count int) {
		ctx := context.Background()
		for i := 1; i <= count; i++ {
			payloadJSON := fmt.Sprintf(`{"action": "bulk_batch_import", "item_num": %d, "duration_ms": 1000}`, i)
			var jobID string
			err := e.db.Pool.QueryRow(ctx, `
				INSERT INTO jobs (tenant_id, payload, status)
				VALUES ($1, $2::jsonb, 'PENDING')
				RETURNING id;
			`, tenantID, payloadJSON).Scan(&jobID)

			if err == nil {
				e.events.Publish(api.Event{
					Type:      "JOB_ENQUEUED",
					JobID:     jobID,
					TenantID:  tenantID,
					Message:   fmt.Sprintf("Chaos flood batch task %d/%d enqueued", i, count),
				})
			}
			time.Sleep(10 * time.Millisecond) // Slight stagger
		}
	}(req.TenantID, req.Count)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "enqueued",
		"tenant_id": req.TenantID,
		"count":     req.Count,
		"message":   fmt.Sprintf("Triggered background flood of %d tasks for tenant %s", req.Count, req.TenantID),
	})
}

// LatencyRequest payload.
type LatencyRequest struct {
	DelayMs int64 `json:"delay_ms"`
}

// HandleLatency toggles simulated latency.
func (e *Engine) HandleLatency(w http.ResponseWriter, r *http.Request) {
	var req LatencyRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	atomic.StoreInt64(&e.simLatency, req.DelayMs)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"simulated_delay_ms": req.DelayMs,
		"status":             "configured",
	})
}
