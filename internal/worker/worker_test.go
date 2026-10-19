package worker

import (
	"context"
	"os"
	"testing"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"
	"aegis/internal/scheduler"
)

func TestWorkerHeartbeatIntervalConfig(t *testing.T) {
	// 1. Default when unset
	_ = os.Unsetenv("HEARTBEAT_INTERVAL_SEC")
	pool := New(nil, nil, nil, 2)
	if pool.heartbeatInterval != 5*time.Second {
		t.Errorf("expected default 5s interval, got %v", pool.heartbeatInterval)
	}

	// 2. Custom env var
	_ = os.Setenv("HEARTBEAT_INTERVAL_SEC", "2")
	defer func() { _ = os.Unsetenv("HEARTBEAT_INTERVAL_SEC") }()
	pool2 := New(nil, nil, nil, 2)
	if pool2.heartbeatInterval != 2*time.Second {
		t.Errorf("expected 2s interval from env, got %v", pool2.heartbeatInterval)
	}

	// 3. SetHeartbeatInterval helper
	pool2.SetHeartbeatInterval(1 * time.Second)
	if pool2.heartbeatInterval != 1*time.Second {
		t.Errorf("expected 1s interval after setter, got %v", pool2.heartbeatInterval)
	}
}

func TestWorkerFencingRejection(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/aegis?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	database, err := db.New(ctx, dbURL)
	if err != nil {
		t.Skipf("Skipping DB integration test: %v", err)
		return
	}
	defer database.Close()

	events := api.NewEventBus()
	pool := New(database, events, nil, 1)

	// 1. Insert a test job
	var jobID string
	err = database.Pool.QueryRow(ctx, `
		INSERT INTO jobs (tenant_id, payload, status, attempts, max_attempts)
		VALUES ('tenant-alpha', '{"action": "test_fencing"}', 'RUNNING', 1, 3)
		RETURNING id;
	`).Scan(&jobID)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	// 2. Simulate Reaper reclaiming the job back to PENDING (e.g. heartbeat expired)
	_, err = database.Pool.Exec(ctx, `UPDATE jobs SET status = 'PENDING' WHERE id = $1`, jobID)
	if err != nil {
		t.Fatalf("failed to update job status to PENDING: %v", err)
	}

	// 3. Zombie worker attempts to complete the job with its stale attempt count (attempt 1)
	task := scheduler.JobTask{
		ID:          jobID,
		TenantID:    "tenant-alpha",
		Attempts:    1,
		MaxAttempts: 3,
	}
	pool.handleTaskSuccess(ctx, task, "zombie-worker")

	// 4. Assert job status is STILL PENDING (fencing prevented zombie from overwriting to COMPLETED)
	var currentStatus string
	err = database.Pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&currentStatus)
	if err != nil {
		t.Fatalf("failed to query job status: %v", err)
	}
	if currentStatus != "PENDING" {
		t.Errorf("expected job status PENDING, got %s (fencing failed to protect reclaimed job)", currentStatus)
	}

	// 5. Assert WORKER_FENCING_REJECTED audit log entry exists
	var count int
	_ = database.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM execution_logs
		WHERE job_id = $1 AND event = 'WORKER_FENCING_REJECTED';
	`, jobID).Scan(&count)
	if count == 0 {
		t.Errorf("expected WORKER_FENCING_REJECTED log event, found none")
	}
}
