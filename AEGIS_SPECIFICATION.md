# AEGIS Runner: System Specification & Developer Handover Blueprint

> Notice for Development Team: This document is the single source of truth for implementing AEGIS Runner. It covers the plain-language core mechanics, problem statement, architectural specification, module-by-module requirements, UI design rules, critical pitfalls to avoid, evaluator defense strategies, and a step-by-step developer build guide. No emojis or decorative gradients are permitted in this codebase or its user interfaces.

---

## 1. What Are We Building? (Plain English Summary)

At its simplest:

1. **A Single Container:** We are packaging an entire background task processing service into a single container running on Azure Container Apps.
2. **Accepts Workloads:** It accepts tasks via a REST API (or through the embedded dashboard) such as billing runs, report generation, or notification dispatches.
3. **Database as Single Source of Truth:** It writes every task and its current lifecycle state directly into a managed relational database (Azure Database for PostgreSQL / RDS).
4. **Crash-Proof State:** The container holds zero persistent state in memory. If the container abruptly dies, restarts, or is terminated by Azure spot reclamation:
   - No data is lost.
   - When the container boots back up (or another worker replica checks in), it queries the database, detects tasks whose heartbeat died, automatically rescues them, and resumes processing.
5. **Two Built-In Protections:**
   - **Fairness (Deficit Round-Robin):** If Tenant A suddenly submits 1,000 batch jobs, Tenant B's single high-priority job is not stuck waiting. The engine schedules work fairly between tenants.
   - **No Duplicates (Content-Based Idempotency):** If an external client retries an API call and sends the exact same task twice, AEGIS detects the duplicate key and payload hash, returning the existing task handle rather than executing twice (preventing duplicate billing or duplicate emails).

---

## 2. Problem Statement (PS)

### Title
**AEGIS Runner: Fault-Tolerant, Fair-Share Asynchronous Task Orchestration Engine with Live Resilience Verification**

### Background and Core Problem
Modern cloud engineering teams running background operations—including billing runs, customer alerts, data pipelines, and analytical report rendering—struggle with silent execution failures and operational opacity. 

When background workers terminate unexpectedly during container eviction or spot instance reclaiming, task state becomes uncertain. Teams face duplicate job executions, resulting in customers receiving duplicate emails or duplicate credit card charges. Furthermore, unthrottled bulk workloads submitted by a single customer choke shared worker capacity, and troubleshooting remains fragmented across disparate queues and logs without unified visibility.

### Critical Failure Vectors Addressed

1. **Ghost Executions:**
   - Symptom: Silent container terminations leave tasks locked in undefined or orphaned states.
   - Impact: Dropped payloads, stuck processing pipelines, or duplicate billing re-runs upon manual operator intervention.

2. **Noisy Neighbor Bursts:**
   - Symptom: High-volume batch imports submitted by a single tenant saturate worker pools.
   - Impact: Worker starvation for time-sensitive, critical transactional workloads (for example, password reset emails, payment webhooks) from other tenants.

3. **Observability Voids:**
   - Symptom: Fragmented logging and disjointed metrics across isolated queues, storage brokers, and dead-letter queues.
   - Impact: Operators are forced to piece together disparate tools (such as CloudWatch, Redis metrics, DB logs) to trace why a mission-critical job stalled or failed.

---

## 3. Abstract of the Problem & Solution

### Executive Abstract
AEGIS Runner provides a unified, production-ready asynchronous job engine packaged as a single deployable container on Azure Container Apps (ACA), backed durably by Azure Database for PostgreSQL Flexible Server. 

Utilizing atomic PostgreSQL row-level locks (`FOR UPDATE SKIP LOCKED`), AEGIS delivers strict at-least-once processing alongside content-based idempotency gates to prevent duplicate executions. Built-in Deficit Round-Robin (DRR) scheduling guarantees tenant fairness and eliminates noisy-neighbor starvation, while an integrated web dashboard streams live execution transitions over Server-Sent Events (SSE). 

To substantiate resilience claims, AEGIS embeds an active Chaos Engine that empowers operators to kill workers and inject queue floods live in real-time, verifying zero-drop recovery without losing a single job.

### Architecture & Cloud Rationale (Azure Container Apps vs. AWS)

| Decision Vector | AWS Equivalent Approach | Azure Container Apps (AEGIS Selected) |
| :--- | :--- | :--- |
| **Container Runtime** | ECS Fargate (requires ALBs, Target Groups, multiple IAM roles) | **UNIFIED PAAS:** Native Envoy ingress and KEDA scaling out of the box |
| **Egress & Baseline Cost** | Private subnets demand AWS NAT Gateways (~$32/mo flat overhead) | **COST-OPTIMIZED:** Built-in secure outbound routing without NAT taxes |
| **Streaming (SSE / WS)** | Complex ALB idle timeout calibration and drop renegotiations | **ZERO-CONFIG:** Long-lived HTTP/2 event streams supported natively |
| **IaC & Deploy Footprint** | Often fragments into SQS + Lambda + Step Functions + EventBridge | **CLEAN MONOLITH:** 1 ACA container + 1 PostgreSQL Terraform module |

---

## 4. UI Design Rules & Standards

To ensure the web dashboard looks professional, modern, and technical (similar to Linear, Vercel, Grafana, or Cloudflare), the following rules are strictly enforced:

1. **Zero Emojis:** Do not use any emoji characters in the user interface, labels, buttons, notifications, or documentation.
2. **No Juvenile Gradients:** Avoid rainbow or novelty gradients. Use a refined, modern dark mode palette (slate, zinc, or neutral monochrome) with subtle border contrast (`border-zinc-800`, `bg-zinc-900`, `text-zinc-100`).
3. **Technical Status Badges:** Use clean geometric indicators:
   - Active / Operational: Small solid green dot (`bg-emerald-500`) with uppercase monospace label `RUNNING` or `HEALTHY`.
   - Pending / Queued: Small neutral dot (`bg-zinc-400`) with label `PENDING`.
   - Stalled / Reclaiming: Amber badge (`bg-amber-500/10 text-amber-400 border border-amber-500/20`) with label `RECLAIMING`.
   - Failed / Interrupted: Crimson badge (`bg-rose-500/10 text-rose-400 border border-rose-500/20`) with label `FAILED`.
4. **Monospace Data Display:** Use monospace typography (`font-mono`, e.g., JetBrains Mono, Inter, or SF Mono) for IDs, timestamps, payload hashes, and latency metrics.
5. **Clear Visual Panels:** The dashboard should feature distinct panels with crisp dividing borders:
   - System Health and Global Counters Bar (Total Ingested, Running, Completed, Recovered, Dropped: 0)
   - Task Submission Form (Inputs for Tenant ID, Action, Payload JSON, with buttons `Submit Task`, `Flood Batch`, `Send Transactional`)
   - Real-Time Execution Stream (Live feed updating via Server-Sent Events)
   - Tenant Fairness Monitor (Comparative metric bars between Tenant A and Tenant B)
   - Chaos Control Console (Action buttons: `Kill Active Worker`, `Inject 500ms DB Latency`)

---

## 5. What to Implement (Module-Wise Architecture)

```
+---------------------------------------------------------------------------------------+
|                       AEGIS RUNNER CONTAINER (Single Binary/App)                      |
|                                                                                       |
|   +-------------------+     +-------------------------+     +---------------------+   |
|   | Module 2:         |     | Module 3:               |     | Module 4 & 5:       |   |
|   | Ingestion &       | --> | Fair-Share Scheduler    | --> | Worker Pool, Lease  |   |
|   | Idempotency Gate  |     | (Deficit Round-Robin)   |     | Heartbeat & Reaper  |   |
|   +-------------------+     +-------------------------+     +---------------------+   |
|             |                            |                             |              |
|             v                            v                             v              |
|   +-------------------------------------------------------------------------------+   |
|   | Module 1: Relational Persistence Engine (PostgreSQL FOR UPDATE SKIP LOCKED)   |   |
|   +-------------------------------------------------------------------------------+   |
|             |                            |                             |              |
|             +----------------------------+-----------------------------+              |
|                                          |                                            |
|                                          v                                            |
|   +---------------------+     +-------------------------+     +-------------------+   |
|   | Module 6:           |     | Module 7:               |     | Module 8:         |   |
|   | Real-Time Event Bus | --> | Live Web Dashboard      | <-> | Chaos Injection   |   |
|   | & SSE Streaming     |     | (Single-Page UI)        |     | Engine            |   |
|   +---------------------+     +-------------------------+     +-------------------+   |
+---------------------------------------------------------------------------------------+
| Module 9: Deployment & Infrastructure as Code (Terraform + Azure Container Apps)      |
+---------------------------------------------------------------------------------------+
```

---

### Module 1: Durable Persistence & Queue Engine (PostgreSQL)
*Responsible for bulletproof task storage, transactional state transitions, and lock-free queue dequeueing.*

- **Data Models / Schema:**
  - `tenants`: Tenant profile, weight/credit rate, max concurrency quota, status.
  - `jobs`: Unique Job ID, tenant ID, queue name, payload (JSONB), status (`PENDING`, `SCHEDULED`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`), priority, attempt count, max retries, execution timeout, scheduled_for, created_at, started_at, finished_at.
  - `job_idempotency_keys`: Idempotency key, tenant ID, payload hash (SHA-256), job ID reference, response cache (JSONB), expires_at.
  - `worker_leases`: Worker ID, job ID, lease acquired at, heartbeat timestamp, lease expiration duration (TTL = 15s, heartbeat = 5s).
  - `execution_logs`: Structured step logs and errors emitted per job execution.
- **Queue Operations:**
  - Atomic dequeue using PostgreSQL `FOR UPDATE SKIP LOCKED` inside a short-lived transaction.
  - Guarantees zero lock contention between concurrent worker threads.
  - Indexes on `(tenant_id, status, scheduled_for)` and `(status, scheduled_for)`.

---

### Module 2: Ingestion & Content-Based Idempotency Gate
*Responsible for accepting tasks via REST, validating inputs, and filtering duplicate execution requests.*

- **REST API Endpoints:**
  - `POST /api/v1/jobs`: Enqueue single task with headers `X-Idempotency-Key` and `X-Tenant-Id`.
  - `POST /api/v1/jobs/batch`: Bulk task submission with per-item idempotency.
  - `GET /api/v1/jobs/{id}`: Task status and result polling.
- **Idempotency Gate Logic:**
  - Computes deterministic SHA-256 hash of the canonical request payload.
  - Checks `job_idempotency_keys` table within an atomic transaction.
  - **Match Found:**
    - If job is still running/completed: Returns the existing Job ID and current status immediately without re-enqueuing.
    - If payload hash mismatches for the same key: Returns `409 Conflict` (idempotency key collision with differing payload).
  - **No Match:** Inserts idempotency lock and persists task as `PENDING`.

---

### Module 3: Fair-Share Scheduler (Deficit Round-Robin)
*Responsible for tenant isolation, fair throughput allocation, and eliminating noisy-neighbor starvation.*

- **Deficit Round-Robin (DRR) Scheduling Algorithm:**
  - Maintains per-tenant deficit counters and configured quantum (credits per round based on tenant tier/weight).
  - Each scheduling tick:
    - Increments active tenant deficit counters by their quantum.
    - Selects candidate tasks from tenants with positive deficit credits up to their concurrency limits.
    - Deducts task cost (normalized weight or execution cost) from the tenant's deficit counter.
- **Tenant Quotas & Rate Throttling:**
  - Strict enforcement of max concurrent running jobs per tenant.
  - Prevents bulk batch imports from starving low-latency, transactional tenants.

---

### Module 4: Worker Pool & Lease Heartbeat Engine
*Responsible for executing workloads, maintaining active liveness leases, and handling retries.*

- **Worker Concurrency Management:**
  - Configurable worker pool managing concurrent execution loops.
  - Cooperative job execution with timeout context cancellation.
- **Lease Heartbeating (Engineered Guarantee 1):**
  - Upon taking a job, the worker writes an execution lease record with a 15-second TTL.
  - An asynchronous background heartbeat loop renews the lease timestamp every 5 seconds while the task runs.
  - On clean task completion or controlled failure, the lease is released and final state is written transactionally.
- **Retry Mechanism:**
  - Configurable retry policy per job type (exponential backoff with jitter).
  - Dead-letter handling once `attempt_count >= max_retries`.

---

### Module 5: Heartbeat Reaper & Crash Recovery Engine (Zero-Drop)
*Responsible for detecting dead/evicted workers and recovering orphaned tasks safely.*

- **Periodic Reaper Loop:**
  - Sweeps the `worker_leases` and `jobs` tables every 5 to 10 seconds.
  - Identifies jobs marked as `RUNNING` whose heartbeat has lapsed beyond the TTL threshold (`heartbeat_at < NOW() - INTERVAL '15 seconds'`).
- **Safe Recovery Protocol:**
  - Releases stale worker locks.
  - Increments job attempt count and appends an execution log record: `Worker stalled / heartbeat expired`.
  - Re-queues the job as `PENDING` (or routes to Dead Letter if max retries reached).
  - **Result:** Guarantees zero-drop task execution even when Azure spot nodes or container replicas terminate without graceful shutdown signals.

---

### Module 6: Real-Time Event Bus & Server-Sent Events (SSE) Engine
*Responsible for pushing state transitions and metrics directly to clients without polling.*

- **Event Pipeline:**
  - Emits real-time state change events (`JOB_ENQUEUED`, `JOB_SCHEDULED`, `JOB_STARTED`, `HEARTBEAT_TICK`, `JOB_COMPLETED`, `JOB_FAILED`, `WORKER_KILLED`, `REAPER_RECOVERED`).
  - Utilizes PostgreSQL `LISTEN / NOTIFY` or internal pub/sub event channels to decouple worker threads from streaming connections.
- **Streaming HTTP Endpoint:**
  - `GET /api/v1/stream/events`: Long-lived HTTP/2 event stream sending structured JSON payloads to browser clients.
  - Keeps connection alive with periodic ping frames, fully supported natively by Azure Container Apps' Envoy ingress.

---

### Module 7: Unified Web Dashboard & Observability UI
*Single integrated web interface served directly from the AEGIS container for end-to-end operational control.*

- **Live Pipeline Visualizer:**
  - Real-time pipeline state showing Queued, Scheduled, Active Workers, Completed, and Dead-letter counts.
  - Visual stream of execution transitions updating instantaneously via SSE.
- **Tenant Fair-Share Inspector:**
  - Comparative gauge and progress charts displaying tenant deficit credits, active running tasks vs. limits, and queue latency per tenant.
- **Job Inspector & Traceability:**
  - Search and filter jobs by ID, tenant, status, or idempotency key.
  - View full execution history, payload, error stack traces, and worker assignment logs.
- **Job Submission Studio:**
  - Interactive form to trigger single or bulk task submissions for quick testing.

---

### Module 8: Live Chaos Verification Engine
*Embedded testing suite empowering operators and evaluators to trigger real-time failure conditions directly from the UI.*

- **Chaos Capabilities:**
  - **Kill Worker Routine (`POST /api/v1/chaos/kill-worker`):**
    - Abruptly aborts a specific active worker thread or process simulating SIGKILL or container preemption.
    - Enables operators to watch the Heartbeat Reaper identify the orphan and recover the task live on the dashboard.
  - **Queue Flood Injection (`POST /api/v1/chaos/flood`):**
    - Injects hundreds of batch tasks from a single noisy tenant.
    - Concurrently submits single transactional tasks from another tenant to prove Deficit Round-Robin fairness in real-time.
  - **Latency / Transient Fault Injection:**
    - Injects artificial database delays or worker handler exceptions to verify backoff and retry behavior.
- **Live Resilience Metrics:**
  - Displays "Zero-Drop Verification Counter": total jobs recovered vs total jobs dropped (target: 0 dropped).

---

### Module 9: Deployment & Infrastructure as Code (Terraform & Azure)
*Single-command, reproducible cloud deployment targeting Azure Container Apps and PostgreSQL.*

- **Single Container Packaging:**
  - Multi-stage Dockerfile containing the API server, fair scheduler, worker pool, reaper, and embedded web UI assets.
  - Zero external queuing dependencies (no Redis, no RabbitMQ, no Kafka needed).
- **Terraform Module Configuration (`/terraform`):**
  - `azurerm_resource_group`: Dedicated resource group.
  - `azurerm_postgresql_flexible_server` & `azurerm_postgresql_flexible_server_database`: Managed PostgreSQL instance with automated backups and private networking.
  - `azurerm_container_app_environment`: Log Analytics workspace and ACA managed environment.
  - `azurerm_container_app`: Single container deployment with native Envoy ingress, HTTPS certificate, environment variables, and health probes (`/healthz`, `/readyz`).

---

## 6. Recommended Tech Stack

To ensure the system compiles into a single, lightweight, zero-dependency container, the recommended tech stack is:

### Primary Option: Go (Golang 1.22+)
* **Why Go:**
  - Native concurrency model (goroutines + channels) fits workers, lease timers, and background reapers with minimal memory overhead.
  - Compiles into a single static binary (~25MB) with zero runtime dependencies.
  - Built-in `embed.FS` packs the frontend HTML/JS/CSS directly into the binary.
  - Cold-start times on Azure Container Apps under 1 second.
* **Key Packages:**
  - HTTP & Routing: `chi` or standard library `net/http`.
  - Database Driver: `pgx/v5` (connection pooling and `LISTEN/NOTIFY` support).
  - Schema Migrations: Embedded SQL migration scripts executed at server bootstrap.

### Alternative Option: TypeScript / Node.js (v20+ LTS)
* Fastify or Express for HTTP/SSE endpoints.
* `pg` (node-postgres) with connection pooling.
* Single-bundle frontend served via static file handler.

---

## 7. Critical Implementation Pitfalls & Avoidance Strategies

### Pitfall 1: Holding PostgreSQL Transactions Open During Job Execution
* **The Mistake:** Opening a transaction, executing `SELECT ... FOR UPDATE SKIP LOCKED`, running the job payload (which might take 10 seconds), and then committing `UPDATE jobs SET status = 'COMPLETED'`.
* **The Danger:** Exhausts the PostgreSQL connection pool, keeps table locks alive, causes transaction wraparound risks, and blocks auto-vacuum.
* **The Solution:** Keep transactions ultra-short (< 5ms):
  1. Transaction 1 (Dequeue): `SELECT id FROM jobs WHERE status = 'PENDING' ... FOR UPDATE SKIP LOCKED LIMIT 1;` followed by `UPDATE jobs SET status = 'RUNNING' WHERE id = ...;` -> **COMMIT IMMEDIATELY**.
  2. Execution Phase: Worker runs payload out-of-band in user space. Background ticker updates `worker_leases.heartbeat_at`.
  3. Transaction 2 (Completion): `UPDATE jobs SET status = 'COMPLETED', finished_at = NOW() WHERE id = ...; DELETE FROM worker_leases WHERE job_id = ...;` -> **COMMIT**.

### Pitfall 2: Chaos "Kill Worker" Accidentally Triggering Graceful Cleanup
* **The Mistake:** Using standard exception handling or `defer`/`finally` blocks that intercept the kill signal and mark the job as `FAILED` or cleanly release the lease.
* **The Danger:** If the worker cleans up after itself, the evaluator will not see the Heartbeat Reaper kick in to rescue an orphaned job.
* **The Solution:**
  - The Chaos Kill API (`POST /api/v1/chaos/kill-worker`) must cancel the worker context abruptly and immediately abort the goroutine/thread without executing the database completion query or lease release.
  - The lease row MUST remain in the database with a frozen timestamp so that the Reaper identifies it after 15 seconds.

### Pitfall 3: Database CPU Spikes from Deficit Round-Robin (DRR) Scheduling
* **The Mistake:** Executing a complex SQL query that recalculates deficit math, tenant balances, and priorities across all tenants on every 100ms scheduler tick.
* **The Danger:** Database CPU hits 100% when there are 1,000+ jobs in the queue.
* **The Solution:**
  - Keep active tenant deficit counters in an in-memory scheduler loop inside the container.
  - On each tick, the in-memory DRR algorithm picks the next tenant ID eligible for execution.
  - Then run a simple, highly indexed targeted query:
    ```sql
    SELECT id, payload FROM jobs 
    WHERE tenant_id = $1 AND status = 'PENDING' 
    ORDER BY created_at ASC 
    LIMIT 1 FOR UPDATE SKIP LOCKED;
    ```

### Pitfall 4: SSE Stream Dropping & Proxy Buffering on Azure Envoy
* **The Mistake:** Opening an SSE connection without disabling HTTP response buffering, or failing to send keep-alive heartbeats.
* **The Danger:** Azure Container Apps' native Envoy proxy will buffer chunks or close idle HTTP connections after 30 to 60 seconds, disconnecting the live dashboard.
* **The Solution:**
  - Always send these exact headers on the SSE endpoint:
    ```http
    Content-Type: text/event-stream
    Cache-Control: no-cache
    Connection: keep-alive
    X-Accel-Buffering: no
    ```
  - Send a heartbeat comment every 15 seconds over the SSE stream: `:keepalive\n\n`.

### Pitfall 5: Idempotency Race Condition on Rapid Submissions
* **The Mistake:** Checking if the key exists (`SELECT ...`), and if null, doing `INSERT INTO jobs ...`.
* **The Danger:** Two concurrent requests with the identical idempotency key arriving milliseconds apart will both pass the check and insert duplicate jobs.
* **The Solution:** Enforce idempotency at the database constraint level:
  ```sql
  INSERT INTO job_idempotency_keys (key, tenant_id, payload_hash, job_id, created_at)
  VALUES ($1, $2, $3, $4, NOW())
  ON CONFLICT (tenant_id, key) DO NOTHING;
  ```
  If zero rows were affected, a duplicate was submitted concurrently. Fetch and return the existing `job_id`.

---

## 8. Strategic Areas for Improvement & Evaluator Defense

### Defense 1: "Why PostgreSQL SKIP LOCKED instead of Redis, SQS, or Kafka?"
* **The Defense:**
  - *No Split-Brain / Atomic Commit:* In financial, billing, or enterprise workflows, tasks are created as a result of business transactions (e.g., invoice generated -> dispatch payment task). Using PostgreSQL enables transactional outbox or dual-write safety in the same database transaction.
  - *Zero Broker Sprawl:* Redis is in-memory and risks data loss on eviction; SQS does not support custom fair-share Deficit Round-Robin scheduling across arbitrary tenant keys; Kafka is designed for event streams, not dynamic job leasing and state updates.
  - *Cost Efficiency:* Avoids managing 3 separate cloud resources (ALB, SQS/Kafka cluster, worker nodes). 1 Container + 1 Database is cheaper, simpler, and less prone to configuration drift.

### Defense 2: "What is the Throughput Ceiling of PostgreSQL Queues?"
* **The Defense:**
  - AEGIS is optimized for **high-reliability, transactional asynchronous orchestration** (target: 1,000 to 5,000 jobs/sec per PostgreSQL instance), where idempotency, tenant fairness, and zero-drop recovery are critical.
  - For millions of telemetry/clickstream events per second, Kafka/Event Hubs is appropriate; but for transactional tasks (reports, payouts, webhooks, data syncs), AEGIS offers vastly superior consistency and resilience.

### Defense 3: "How does AEGIS scale horizontally?"
* **The Defense:**
  - Azure Container Apps natively integrates KEDA (Kubernetes Event-driven Autoscaling).
  - KEDA scales the AEGIS container replicas out automatically based on the PostgreSQL query metric (`COUNT(*) FROM jobs WHERE status = 'PENDING' > 50`).
  - Because `FOR UPDATE SKIP LOCKED` operates at the row level, multiple container replicas can dequeue concurrently from the shared database without duplicate delivery or deadlocks.

---

## 9. Teammate Handover: Step-by-Step Build Plan

```
[Phase 1] Local DB & Schema  --->  [Phase 2] Ingestion & Idempotency  --->  [Phase 3] Worker & Heartbeat
                                                                                      |
[Phase 6] Live UI & Demo     <---  [Phase 5] SSE Stream & Events     <---  [Phase 4] Reaper & DRR Scheduler
         |
[Phase 7] Chaos Engine       --->  [Phase 8] Docker & Azure Deploy
```

### Phase 1: Local Environment & PostgreSQL Schema
1. Create a `docker-compose.yml` file running PostgreSQL 16 on `localhost:5432`.
2. Create migration scripts defining the core tables:
   ```sql
   CREATE TABLE tenants (
       id VARCHAR(64) PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       quantum INT DEFAULT 10,
       concurrency_limit INT DEFAULT 5,
       created_at TIMESTAMPTZ DEFAULT NOW()
   );

   CREATE TABLE jobs (
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
   CREATE INDEX idx_jobs_tenant_status ON jobs (tenant_id, status, scheduled_for);
   CREATE INDEX idx_jobs_queue ON jobs (status, scheduled_for);

   CREATE TABLE job_idempotency_keys (
       key VARCHAR(255) NOT NULL,
       tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
       payload_hash CHAR(64) NOT NULL,
       job_id UUID NOT NULL REFERENCES jobs(id),
       created_at TIMESTAMPTZ DEFAULT NOW(),
       PRIMARY KEY (tenant_id, key)
   );

   CREATE TABLE worker_leases (
       job_id UUID PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
       worker_id VARCHAR(128) NOT NULL,
       heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
   );

   CREATE TABLE execution_logs (
       id BIGSERIAL PRIMARY KEY,
       job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
       event VARCHAR(64) NOT NULL,
       message TEXT NOT NULL,
       created_at TIMESTAMPTZ DEFAULT NOW()
   );
   ```
3. Seed at least two tenants:
   - `tenant-alpha` (Batch tenant, weight: 10)
   - `tenant-beta` (Transactional tenant, weight: 10)

### Phase 2: Ingestion API & Content-Based Idempotency Gate
1. Implement `POST /api/v1/jobs`:
   - Request Body: `{"tenant_id": "tenant-alpha", "payload": {"action": "send_email", "to": "user@example.com"}}`
   - Header: `X-Idempotency-Key: req-abc-123`
2. Logic:
   - Hash payload: `sha256(canonical_json(payload))`.
   - Query `job_idempotency_keys` for `(tenant_id, key)`.
   - If present and hash matches: Return `200 OK` with existing `job_id` and status.
   - If present and hash differs: Return `409 Conflict`.
   - If not present: Insert into `jobs` (`status = 'PENDING'`), insert into `job_idempotency_keys`, return `202 Accepted` with new `job_id`.

### Phase 3: Worker Pool & 5s Lease Heartbeat Engine
1. Configure worker pool (for example, 5 concurrent worker routines).
2. Atomic Dequeue Query:
   ```sql
   WITH next_job AS (
       SELECT id FROM jobs 
       WHERE status = 'PENDING' AND scheduled_for <= NOW()
       ORDER BY scheduled_for ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED
   )
   UPDATE jobs j
   SET status = 'RUNNING', started_at = NOW(), attempts = attempts + 1
   FROM next_job
   WHERE j.id = next_job.id
   RETURNING j.id, j.tenant_id, j.payload, j.attempts;
   ```
3. Immediately insert lease:
   ```sql
   INSERT INTO worker_leases (job_id, worker_id, heartbeat_at)
   VALUES ($1, $2, NOW())
   ON CONFLICT (job_id) DO UPDATE SET heartbeat_at = NOW();
   ```
4. Start a 5-second interval ticker in the worker routine:
   - Executes `UPDATE worker_leases SET heartbeat_at = NOW() WHERE job_id = $1`.
5. Execute job task (simulate 5 to 10 seconds of processing).
6. On success:
   - Mark `jobs.status = 'COMPLETED'`.
   - Delete from `worker_leases`.

### Phase 4: Heartbeat Reaper & Deficit Round-Robin (DRR) Scheduler
1. **Reaper Background Daemon:**
   - Runs every 5 seconds.
   - SQL query to rescue orphaned jobs:
     ```sql
     WITH expired_leases AS (
         SELECT job_id FROM worker_leases 
         WHERE heartbeat_at < NOW() - INTERVAL '15 seconds'
     ),
     reclaimed_jobs AS (
         UPDATE jobs
         SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END
         WHERE id IN (SELECT job_id FROM expired_leases)
         RETURNING id, attempts, max_attempts
     )
     DELETE FROM worker_leases WHERE job_id IN (SELECT job_id FROM expired_leases);
     ```
   - Log `REAPER_RECOVERED` event into `execution_logs`.
2. **Deficit Round-Robin Fair Scheduler:**
   - In memory, maintain tenant deficit counters:
     - For each round: `tenant.deficit += tenant.quantum`.
     - Dequeue jobs for tenants with `deficit >= job_cost`.
     - Prevents `tenant-alpha` from starving `tenant-beta`.

### Phase 5: Real-Time Event Bus & Server-Sent Events (SSE)
1. Implement a thread-safe broadcast channel in memory (or PostgreSQL `LISTEN / NOTIFY`).
2. When state changes occur (`JOB_ENQUEUED`, `JOB_RUNNING`, `HEARTBEAT_TICK`, `JOB_COMPLETED`, `WORKER_KILLED`, `REAPER_RECOVERED`), publish an event:
   ```json
   {
     "event": "JOB_RUNNING",
     "job_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
     "tenant_id": "tenant-beta",
     "timestamp": "2026-10-17T18:00:00Z"
   }
   ```
3. Expose `GET /api/v1/stream/events` with proper headers and 15s keep-alive pings.

### Phase 6: Embedded Web Dashboard (Single-Page UI)
Create a clean, responsive single HTML file (Tailwind CSS, dark slate palette, zero emojis) served at `GET /`:
- Top Status Bar: System Health (Operational), Active Workers, Recovered Counter, Dropped Counter (0).
- Task Studio Form: Tenant selector, payload textarea, submit buttons: `[Submit Task]`, `[Flood 200 Tasks]`, `[Send 1 Priority Task]`.
- Real-Time Execution Table: Monospace log connecting directly to `/api/v1/stream/events`.
- Tenant Fairness Visualizer: Comparative progress bars showing queue balance.
- Chaos Control Panel: Buttons: `[Kill Active Worker]`, `[Simulate DB Delay]`.

### Phase 7: Live Chaos Engine Endpoints
1. `POST /api/v1/chaos/kill-worker`:
   - Finds an active worker running a job.
   - Immediately cancels the execution context **without** releasing the DB lease.
   - The worker stops heartbeating.
   - Emits `WORKER_KILLED` event over SSE.
   - 15 seconds later, the Reaper detects the orphan, re-queues it, another worker picks it up, and it completes without data loss.
2. `POST /api/v1/chaos/flood`:
   - Spawns a background routine that enqueues 200 tasks for `tenant-alpha`.
   - Allows testing DRR fairness live on the dashboard.

### Phase 8: Dockerfile & Azure Deployment
1. Multi-stage Dockerfile producing a lean static container image.
2. Deploy to Azure using Azure CLI:
   ```bash
   az group create --name aegis-rg --location eastus
   az acr create --resource-group aegis-rg --name aegisregistry --sku Basic --admin-enabled true
   az acr build --registry aegisregistry --image aegis-runner:v1 .

   az postgres flexible-server create \
     --resource-group aegis-rg \
     --name aegis-postgres-db \
     --admin-user aegisadmin \
     --admin-password "StrongPassword123!" \
     --sku-name Standard_B1ms \
     --tier Burstable

   az containerapp create \
     --name aegis-runner \
     --resource-group aegis-rg \
     --image aegisregistry.azurecr.io/aegis-runner:v1 \
     --environment aegis-env \
     --ingress external \
     --target-port 8080 \
     --env-vars DATABASE_URL="postgres://aegisadmin:StrongPassword123!@aegis-postgres-db.postgres.database.azure.com:5432/postgres?sslmode=require"
   ```

---

## 10. Evaluation Rubric & Quality Assurance Guide

### Pillar 1: Functionality (Weight: 25% | Target: 10/10)
- Strict At-Least-Once Processing: Tasks must never be dropped under worker crashes.
- Content-Based Idempotency Gate: Returns existing cached response on duplicate `(tenant_id, idempotency_key)` and payload; returns `409 Conflict` on payload mismatch.
- Deficit Round-Robin (DRR) Fair-Share: Prevents starvation of low-volume tenants during bursts.
- Active Worker Lease & Heartbeat Loop: Workers refresh a 5-second lease in PostgreSQL.
- Heartbeat Reaper Daemon: Sweeps leases expired past 15 seconds and re-queues tasks.
- Real-Time Web Dashboard: Live streaming over Server-Sent Events (SSE).
- Interactive Chaos Engine: Live `Kill Worker` and `Queue Flood` controls demonstrating zero-drop recovery.

### Pillar 2: Code Quality & Architecture (Weight: 20% | Target: 9.5/10)
- Clean package division (`internal/api`, `internal/scheduler`, `internal/worker`, `internal/reaper`, `internal/db`).
- Context cancellation propagation (`context.Context`) across all queries and routines.
- Explicit error handling with context wrapping.
- Zero mutable global state; clean dependency injection via constructors.
- Zero goroutine/memory leaks with clean ticker terminations.

### Pillar 3: Security & Multi-Tenancy (Weight: 20% | Target: 9.5/10)
- Multi-tenant data segregation with `WHERE tenant_id = $1` on every query.
- 100% parameterized SQL queries (zero string formatting).
- Request body size limits (max 1MB).
- TLS 1.2+ PostgreSQL connections and credentials managed via Azure secrets.
- Non-root container execution (`USER 1001:1001`).

### Pillar 4: Testing & Verification Strategy (Weight: 20% | Target: 9.5/10)
- Unit tests for DRR scheduling algorithm and SHA-256 payload canonicalization.
- Concurrency testing with `go test -race ./...`.
- Atomic dequeue test verifying zero double-assignment under 50 simultaneous workers.
- Automated Chaos test: aborts worker and asserts the Reaper reclaims and completes the task (`attempts == 2`).

### Pillar 5: Individual Contribution & Team Division (Weight: 15% | Target: 10/10)

#### Role 1: Systems Architect & Product Lead (Your Contribution)
- Authored the system specification, architectural topology, and data contracts.
- Designed distributed resilience models: 5s lease heartbeating, 15s heartbeat reaper, and PostgreSQL `FOR UPDATE SKIP LOCKED` concurrency patterns.
- Formulated the Deficit Round-Robin (DRR) scheduling algorithm and tenant quota model.
- Authored failure vector mitigation strategies and evaluator defense arguments.
- Designed Chaos Engine specifications and the 3-minute live demonstration workflow.
- Conducted asynchronous code reviews on PRs, enforcing code quality, security checks, and rubric compliance.

#### Role 2: Core Systems & Cloud Infrastructure Engineer (Teammate's Contribution)
- Implemented the Go/TypeScript backend, PostgreSQL migrations, and connection pool layer.
- Built the REST Ingestion API, content-based idempotency hashing, and short-transaction dequeue logic.
- Implemented the background Worker Pool, Heartbeat Reaper daemon, and DRR fair-scheduler loop.
- Developed the live Server-Sent Events (SSE) broadcaster and interactive single-page dashboard.
- Authored unit, concurrency race, and integration test suites.
- Created the multi-stage Dockerfile and Terraform/Azure CLI infrastructure scripts.

#### Git Collaboration Hygiene:
- Work on feature branches (`feature/db-schema`, `feature/idempotency`, `feature/reaper`, `feature/ui`).
- Open Pull Requests with architectural review comments before merging into `main`.
- Use conventional commit messages (`feat(worker): implement 5s heartbeat loop`, `test(concurrency): add skip-locked race condition test`).

---

## 11. Live Demonstration Script for Evaluators (3 Minutes)

1. **Step 1: Normal Operation (30s)**
   - Open the web dashboard.
   - Click `[Submit Task]`.
   - Point out instantaneous SSE state transition (`PENDING` -> `RUNNING` with active 5s heartbeat -> `COMPLETED`).
2. **Step 2: Fair-Share Verification (60s)**
   - Click `[Flood 200 Tasks (Tenant A)]`.
   - Immediately click `[Send 1 Priority Task (Tenant B)]`.
   - Point to the screen: Tenant B's job is not blocked behind Tenant A's 200 tasks; the DRR scheduler processes Tenant B immediately.
3. **Step 3: Chaos Kill and Zero-Drop Recovery (90s)**
   - Start a 20-second simulated task.
   - Show the worker actively heartbeating every 5s on the dashboard.
   - Click `[Kill Active Worker]`.
   - The worker terminates abruptly and its heartbeats cease.
   - Wait 15 seconds: The Heartbeat Reaper detects the orphan, logs `Worker stalled`, reclaims the task, and assigns it to another worker.
   - The task completes cleanly. The live counter reads: `Recovered: 1 | Dropped: 0`.

---

## 12. Repository File Structure

```
AEGIS/
├── AEGIS_SPECIFICATION.md              # Master specification and developer guide
├── BATCH_AND_TENANCY_GUIDE.md          # Guide on batch jobs, Python examples, and tenant isolation
├── Dockerfile                          # Multi-stage container build
├── docker-compose.yml                  # Local development PostgreSQL setup
├── Makefile                            # build, run, test, migrate shortcuts
├── cmd/
│   └── aegis/
│       └── main.go                     # Entrypoint & server bootstrap
├── internal/
│   ├── api/                            # REST ingestion & SSE streaming endpoints
│   ├── chaos/                          # Kill-worker and queue-flood handlers
│   ├── db/                             # SQL migrations and database connection pool
│   ├── idempotency/                    # Payload hashing & deduplication gate
│   ├── reaper/                         # Heartbeat reaper background daemon
│   ├── scheduler/                      # Deficit Round-Robin (DRR) scheduler
│   └── worker/                         # Worker pool & 5-second lease heartbeater
├── web/                                # Frontend dashboard (HTML/CSS/JS)
│   ├── index.html
│   └── app.js
└── terraform/                          # Production Azure Container Apps IaC
    ├── main.tf
    ├── variables.tf
    └── outputs.tf
```
