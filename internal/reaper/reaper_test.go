package reaper

import (
	"context"
	"os"
	"testing"
	"time"

	"aegis/internal/api"
	"aegis/internal/db"
)

func TestReaperReclaimExpiredLease(t *testing.T) {
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

	// 1. Create a job marked as RUNNING with an expired lease (>15s ago)
	var jobID string
	err = database.Pool.QueryRow(ctx, `
		INSERT INTO jobs (tenant_id, payload, status, attempts, max_attempts)
		VALUES ('tenant-alpha', '{"action": "test_crash"}', 'RUNNING', 1, 3)
		RETURNING id;
	`).Scan(&jobID)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	// 2. Insert lease with heartbeat 20 seconds in the past
	_, err = database.Pool.Exec(ctx, `
		INSERT INTO worker_leases (job_id, worker_id, heartbeat_at)
		VALUES ($1, 'worker-dead', NOW() - INTERVAL '20 seconds');
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert expired lease: %v", err)
	}

	// 3. Run reaper sweep
	r := New(database, events, 1*time.Second, 15*time.Second)
	r.sweep(ctx)

	// 4. Verify job was rescued back to PENDING and lease deleted
	var newStatus string
	var attempts int
	err = database.Pool.QueryRow(ctx, "SELECT status, attempts FROM jobs WHERE id = $1", jobID).Scan(&newStatus, &attempts)
	if err != nil {
		t.Fatalf("failed to query reclaimed job: %v", err)
	}

	if newStatus != "PENDING" {
		t.Errorf("expected job status PENDING, got %s", newStatus)
	}

	var leaseCount int
	_ = database.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM worker_leases WHERE job_id = $1", jobID).Scan(&leaseCount)
	if leaseCount != 0 {
		t.Errorf("expected 0 worker leases remaining, got %d", leaseCount)
	}
}
