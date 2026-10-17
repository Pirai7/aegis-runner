CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantum INT DEFAULT 10,
    concurrency_limit INT DEFAULT 5,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    scheduled_for TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status ON jobs (tenant_id, status, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (status, scheduled_for);

CREATE TABLE IF NOT EXISTS job_idempotency_keys (
    key VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    payload_hash CHAR(64) NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, key)
);

CREATE TABLE IF NOT EXISTS worker_leases (
    job_id UUID PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id VARCHAR(128) NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS execution_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_logs_job ON execution_logs (job_id, created_at);

-- Seed initial default tenants
INSERT INTO tenants (id, name, quantum, concurrency_limit)
VALUES 
    ('tenant-alpha', 'Alpha Batch Operations', 10, 5),
    ('tenant-beta', 'Beta Transactional Services', 10, 5),
    ('tenant-billing-corp', 'Billing Corp Enterprise', 10, 5)
ON CONFLICT (id) DO UPDATE 
SET quantum = EXCLUDED.quantum, concurrency_limit = EXCLUDED.concurrency_limit;
