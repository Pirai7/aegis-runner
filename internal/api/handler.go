package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aegis/internal/db"
	"aegis/internal/idempotency"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// SchedulerInterface exposes DRR scheduler methods needed by API handlers.
type SchedulerInterface interface {
	GetTenantMetrics(ctx context.Context) ([]TenantMetric, error)
}

// TenantMetric represents tenant metrics for API response.
type TenantMetric struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Quantum          int    `json:"quantum"`
	Deficit          int    `json:"deficit"`
	ConcurrencyLimit int    `json:"concurrency_limit"`
	ActiveRunning    int    `json:"active_running"`
	PendingCount     int    `json:"pending_count"`
	CompletedCount   int    `json:"completed_count"`
}

// StatsProvider exposes pool metrics.
type StatsProvider interface {
	GetStats(ctx context.Context) map[string]any
	GetActiveJobs() []map[string]string
}

// Handler handles all REST API requests.
type Handler struct {
	db        *db.DB
	events    *EventBus
	stats     StatsProvider
	scheduler SchedulerInterface
}

// NewHandler creates an initialized API handler.
func NewHandler(database *db.DB, events *EventBus, stats StatsProvider, sched SchedulerInterface) *Handler {
	return &Handler{
		db:        database,
		events:    events,
		stats:     stats,
		scheduler: sched,
	}
}

// CreateJobRequest represents the incoming task payload.
type CreateJobRequest struct {
	TenantID string         `json:"tenant_id,omitempty"`
	Action   string         `json:"action,omitempty"`
	Payload  map[string]any `json:"payload,omitempty"`
}

// CreateJob handles POST /api/v1/jobs.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	// Limit request body size to 1MB
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

	tenantID := r.Header.Get("X-Tenant-Id")
	idempotencyKey := r.Header.Get("X-Idempotency-Key")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body or size exceeded 1MB")
		return
	}

	var req CreateJobRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Malformed JSON payload: %v", err))
		return
	}

	if tenantID == "" && req.TenantID != "" {
		tenantID = req.TenantID
	}
	if tenantID == "" {
		tenantID = "tenant-alpha"
	}

	// Canonical payload normalization
	rawPayload := req.Payload
	if rawPayload == nil {
		rawPayload = make(map[string]any)
	}
	if req.Action != "" && rawPayload["action"] == nil {
		rawPayload["action"] = req.Action
	}

	payloadHash, canonicalBytes, err := idempotency.CanonicalizeAndHash(rawPayload)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Failed to process payload: %v", err))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Ensure tenant exists
	var tenantExists bool
	err = h.db.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)", tenantID).Scan(&tenantExists)
	if err != nil || !tenantExists {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Tenant '%s' does not exist", tenantID))
		return
	}

	// 1. Idempotency Check
	if idempotencyKey != "" {
		var existingJobID string
		var storedHash string
		var existingStatus string

		err = h.db.Pool.QueryRow(ctx, `
			SELECT k.job_id, k.payload_hash, j.status
			FROM job_idempotency_keys k
			JOIN jobs j ON j.id = k.job_id
			WHERE k.tenant_id = $1 AND k.key = $2
		`, tenantID, idempotencyKey).Scan(&existingJobID, &storedHash, &existingStatus)

		if err == nil {
			// Key exists: check for payload hash collision
			if storedHash != payloadHash {
				h.writeError(w, http.StatusConflict, "Idempotency key collision: payload hash does not match original submission")
				return
			}
			// Exact match: return existing job cached response
			h.writeJSON(w, http.StatusOK, map[string]any{
				"job_id":  existingJobID,
				"status":  existingStatus,
				"cached":  true,
				"message": "Duplicate submission detected; returning existing task handle",
			})
			return
		} else if !errors.Is(err, pgx.ErrNoRows) {
			h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Database error checking idempotency: %v", err))
			return
		}
	}

	// 2. Insert new job
	var newJobID string
	err = h.db.Pool.QueryRow(ctx, `
		INSERT INTO jobs (tenant_id, payload, status)
		VALUES ($1, $2::jsonb, 'PENDING')
		RETURNING id;
	`, tenantID, string(canonicalBytes)).Scan(&newJobID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to enqueue job: %v", err))
		return
	}

	// 3. Persist idempotency key if provided
	if idempotencyKey != "" {
		_, _ = h.db.Pool.Exec(ctx, `
			INSERT INTO job_idempotency_keys (key, tenant_id, payload_hash, job_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, key) DO NOTHING;
		`, idempotencyKey, tenantID, payloadHash, newJobID)
	}

	// 4. Publish Event
	h.events.Publish(Event{
		Type:     "JOB_ENQUEUED",
		JobID:    newJobID,
		TenantID: tenantID,
		Attempts: 0,
		Message:  fmt.Sprintf("Job enqueued for tenant %s", tenantID),
	})

	h.writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":    newJobID,
		"status":    "PENDING",
		"tenant_id": tenantID,
		"cached":    false,
	})
}

// CreateBatchJobs handles POST /api/v1/jobs/batch.
func (h *Handler) CreateBatchJobs(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-Id")
	if tenantID == "" {
		tenantID = "tenant-alpha"
	}

	var items []map[string]any
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid batch JSON array")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	results := make([]map[string]any, 0, len(items))

	for idx, item := range items {
		itemKey := ""
		if k, ok := item["_idempotency_key"].(string); ok {
			itemKey = k
			delete(item, "_idempotency_key")
		}

		canonicalHash, canonicalBytes, err := idempotency.CanonicalizeAndHash(item)
		if err != nil {
			results = append(results, map[string]any{"index": idx, "error": err.Error()})
			continue
		}

		var jobID string
		err = h.db.Pool.QueryRow(ctx, `
			INSERT INTO jobs (tenant_id, payload, status)
			VALUES ($1, $2::jsonb, 'PENDING')
			RETURNING id;
		`, tenantID, string(canonicalBytes)).Scan(&jobID)

		if err != nil {
			results = append(results, map[string]any{"index": idx, "error": err.Error()})
			continue
		}

		if itemKey != "" {
			_, _ = h.db.Pool.Exec(ctx, `
				INSERT INTO job_idempotency_keys (key, tenant_id, payload_hash, job_id)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (tenant_id, key) DO NOTHING;
			`, itemKey, tenantID, canonicalHash, jobID)
		}

		h.events.Publish(Event{
			Type:     "JOB_ENQUEUED",
			JobID:    jobID,
			TenantID: tenantID,
			Message:  fmt.Sprintf("Batch job %d/%d enqueued", idx+1, len(items)),
		})

		results = append(results, map[string]any{
			"job_id": jobID,
			"status": "PENDING",
		})
	}

	h.writeJSON(w, http.StatusAccepted, map[string]any{
		"total_submitted": len(items),
		"jobs":            results,
	})
}

// GetJob handles GET /api/v1/jobs/{id}.
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	tenantID := r.Header.Get("X-Tenant-Id")

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	query := `
		SELECT j.id, j.tenant_id, j.payload, j.status, j.attempts, j.max_attempts,
		       j.created_at, j.scheduled_for, j.started_at, j.finished_at,
		       wl.worker_id, wl.heartbeat_at
		FROM jobs j
		LEFT JOIN worker_leases wl ON wl.job_id = j.id
		WHERE j.id = $1
	`
	args := []any{jobID}
	if tenantID != "" {
		query += " AND j.tenant_id = $2"
		args = append(args, tenantID)
	}

	var id, tID, status string
	var payload json.RawMessage
	var attempts, maxAttempts int
	var createdAt, scheduledFor time.Time
	var startedAt, finishedAt *time.Time
	var workerID *string
	var heartbeatAt *time.Time

	err := h.db.Pool.QueryRow(ctx, query, args...).Scan(
		&id, &tID, &payload, &status, &attempts, &maxAttempts,
		&createdAt, &scheduledFor, &startedAt, &finishedAt,
		&workerID, &heartbeatAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			h.writeError(w, http.StatusNotFound, "Job not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Fetch execution logs for this job
	logRows, _ := h.db.Pool.Query(ctx, `
		SELECT event, message, created_at
		FROM execution_logs
		WHERE job_id = $1
		ORDER BY created_at ASC
	`, jobID)
	logs := make([]map[string]any, 0)
	if logRows != nil {
		defer logRows.Close()
		for logRows.Next() {
			var evt, msg string
			var ts time.Time
			if err := logRows.Scan(&evt, &msg, &ts); err == nil {
				logs = append(logs, map[string]any{
					"event":      evt,
					"message":    msg,
					"created_at": ts,
				})
			}
		}
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"job_id":        id,
		"tenant_id":     tID,
		"payload":       payload,
		"status":        status,
		"attempts":      attempts,
		"max_attempts":  maxAttempts,
		"created_at":    createdAt,
		"scheduled_for": scheduledFor,
		"started_at":    startedAt,
		"finished_at":   finishedAt,
		"worker_id":     workerID,
		"heartbeat_at":  heartbeatAt,
		"logs":          logs,
	})
}

// ListJobs handles GET /api/v1/jobs.
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	status := r.URL.Query().Get("status")
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
		limit = l
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := `
		SELECT j.id, j.tenant_id, j.status, j.attempts, j.created_at, j.started_at, j.finished_at,
		       wl.worker_id, wl.heartbeat_at
		FROM jobs j
		LEFT JOIN worker_leases wl ON wl.job_id = j.id
		WHERE 1=1
	`
	args := []any{}
	argIdx := 1

	if tenantID != "" {
		query += fmt.Sprintf(" AND j.tenant_id = $%d", argIdx)
		args = append(args, tenantID)
		argIdx++
	}
	if status != "" {
		query += fmt.Sprintf(" AND j.status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY j.created_at DESC LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := h.db.Pool.Query(ctx, query, args...)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	jobs := make([]map[string]any, 0)
	for rows.Next() {
		var id, tID, st string
		var attempts int
		var createdAt time.Time
		var startedAt, finishedAt *time.Time
		var workerID *string
		var heartbeatAt *time.Time

		if err := rows.Scan(&id, &tID, &st, &attempts, &createdAt, &startedAt, &finishedAt, &workerID, &heartbeatAt); err == nil {
			jobs = append(jobs, map[string]any{
				"id":           id,
				"tenant_id":    tID,
				"status":       st,
				"attempts":     attempts,
				"created_at":   createdAt,
				"started_at":   startedAt,
				"finished_at":  finishedAt,
				"worker_id":    workerID,
				"heartbeat_at": heartbeatAt,
			})
		}
	}

	h.writeJSON(w, http.StatusOK, jobs)
}

// ListTenants handles GET /api/v1/tenants.
func (h *Handler) ListTenants(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	metrics, err := h.scheduler.GetTenantMetrics(ctx)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, metrics)
}

// GetStats handles GET /api/v1/stats.
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	stats := h.stats.GetStats(ctx)
	stats["active_job_details"] = h.stats.GetActiveJobs()

	h.writeJSON(w, http.StatusOK, stats)
}

// Healthz handles GET /healthz without depending on external services.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy","service":"aegis-runner"}`))
}

// Readyz handles GET /readyz and reports whether PostgreSQL is reachable.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.db.Pool.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("DATABASE_UNAVAILABLE"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready","service":"aegis-runner"}`))
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{
		"error": strings.TrimSpace(message),
	})
}
