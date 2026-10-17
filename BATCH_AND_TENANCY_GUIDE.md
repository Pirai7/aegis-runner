# Batch Processing, Multi-Tenant Isolation, and Cloud Architecture Guide

> Domain Track: Cloud: Reliable Asynchronous Job Runner
> Notice: This document explains the operational concepts of batch workloads, what a job actually is, how a Go runner executes tasks and interacts with Python, tenant isolation mechanics, and the architectural comparison between AWS and Azure for the AEGIS Runner orchestration engine. All examples and documentation adhere strictly to professional standards: no emojis and no decorative gradients.

---

## 1. Hackathon Domain: "Cloud: Reliable Asynchronous Job Runner"

Our selected domain for the competition is **Cloud: Reliable Asynchronous Job Runner**.

In enterprise cloud systems, when applications need to perform heavy or delayed operations (such as processing payments, sending bulk alerts, or rendering PDFs), they never do it inside the user's synchronous web request. Instead, they hand it off to an **asynchronous job runner**.

The core requirement of this domain is **reliability**:
- No jobs may be silently lost if a server reboots or spot instances are reclaimed (Zero-Drop guarantee).
- No jobs may be processed twice if network retries occur (Idempotency guarantee).
- No single client may starve the shared workers (Fair-Share guarantee).

---

## 2. What Is a Single "Job"? Is It a Program?

### The Misconception
Many people assume a "job" is an executable file or binary program (such as an `.exe` or a raw `.py` script) uploaded by users. 

### What a Job Actually Is in Production Systems
In production cloud architectures (such as Celery, AWS SQS, Temporal, Sidekiq, and AEGIS), a job is **data**, not a compiled program.

A single job is a **structured task instruction** (stored as JSON in the database) containing:
1. **Action Name:** What function or handler needs to run (e.g., `process_payment`, `send_email`, `generate_invoice`).
2. **Parameters / Payload:** The specific arguments needed to execute that action.
3. **Execution Metadata:** Priority, timeout limit, max retries, and tenant ownership.

### Example of a Real Job in the Database:
```json
{
  "job_id": "7f3e8a12-9c4b-4b91-9876-123456789abc",
  "tenant_id": "tenant-billing-corp",
  "action": "send_welcome_email",
  "payload": {
    "user_id": 98214,
    "email": "customer@example.com",
    "template_id": "tmpl_welcome_v2"
  },
  "status": "PENDING",
  "attempts": 0,
  "max_attempts": 3
}
```

The runner engine (AEGIS) reads this JSON from the database and passes it to the appropriate execution handler.

---

## 3. How Does a Go Program Run a Python One?

If the AEGIS Runner engine is written in Go, how does it interact with or run Python code? There are three standard architectural patterns:

### Pattern A (The Standard Cloud API Pattern - Recommended for AEGIS)
* **How it works:** 
  - The Go container runs the entire engine, database connection, scheduler, and worker pool.
  - Python applications are **clients** that talk to the Go runner via standard REST APIs (`POST /api/v1/jobs`).
  - Example: A Django or FastAPI web app written in Python accepts a user registration, and sends a JSON payload to the AEGIS Go container to send the verification email asynchronously.
  - The Go worker picks up the job and executes the network call or business logic directly. Python never has to run inside the container.

### Pattern B (Go as an Orchestrator Executing Python Subprocesses)
* **How it works:**
  - If a tenant specifically requires a custom Python script to run (for example, a machine learning inference script or data science script like `model_predict.py`):
  - Both Go and Python 3 are installed inside the Docker container.
  - When the Go worker claims a job with `action: "run_python_script"`, the Go program uses Go's standard `os/exec` package to spawn Python as a child process:
    ```go
    // Inside Go worker:
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    cmd := exec.CommandContext(ctx, "python3", "scripts/process_invoice.py", "--payload", payloadJSON)
    output, err := cmd.CombinedOutput()
    if err != nil {
        // Go detects Python process crash or non-zero exit code
        log.Printf("Python script failed: %v", err)
    }
    ```
  - **Crash Protection:** If the Python script runs out of memory (OOM) or crashes, the Go worker catches the error, records the stderr output in `execution_logs`, and marks the job for retry.

### Pattern C (Go Worker Calling a Python Microservice via Webhook / gRPC)
* **How it works:**
  - The Go worker picks up the job from the PostgreSQL queue.
  - Instead of running local code, the Go worker makes an HTTP POST request to a separate Python worker service (`http://python-worker:5000/execute`) with a 15-second timeout.
  - If the Python service responds with 200 OK, the Go worker marks the job `COMPLETED`. If the Python service dies or times out, the Go worker flags the failure and retries.

> For our hackathon demo, **Pattern A** (Go container running built-in simulated handlers, with Python used as the demonstration client script) is the cleanest, fastest, and most robust approach. Pattern B can also be easily added if running an actual local `.py` script is desired.

---

## 4. What Is a Batch Job? (In Plain Language)

### The Concept
In software engineering, tasks fall into two categories:

1. **Interactive (Synchronous) Requests:**
   - A user clicks a button, waits 200 milliseconds, and gets an immediate response on their screen.
   - Example: Loading a user profile page or fetching a shopping cart.
   - If the server crashes during this request, the browser shows a 500 error, and the user refreshes the page.

2. **Batch (Asynchronous) Workloads:**
   - Tasks that require significant computational time, touch large volumes of data, or interact with external third-party rate-limited APIs.
   - These tasks cannot block the user's browser. Instead, they are accepted by an API, acknowledged with a 202 Accepted status code and a unique Job ID, and processed in the background.
   - Examples of real-world batch workloads:
     - End-of-month customer invoicing: Generating 5,000 PDF invoices and calculating sales tax.
     - Bulk notifications: Sending 50,000 transactional emails or SMS alerts after a security update.
     - Data synchronization: Ingesting a 100,000-row CSV file from a supplier and updating warehouse inventory.
     - Analytical report rendering: Aggregating quarterly telemetry logs into an executive summary.

### Why Batch Jobs Need an Engine Like AEGIS
If a worker container crashes halfway through processing 10,000 invoices:
- Without AEGIS: The system loses track of which invoices were sent and which were pending. An operator must guess, risking double-charging customers or dropping jobs completely.
- With AEGIS: Every job is tracked durably in PostgreSQL with 5-second worker leases. If a container dies, the Heartbeat Reaper detects the interruption, resets stalled jobs back to `PENDING`, and another worker completes the remaining work without data loss or duplicates.

---

## 5. Practical Code Example: Python Batch Client

Below is a complete, runnable Python script demonstrating how an external client or application interacts with AEGIS Runner to enqueue a batch of background tasks, enforce content-based idempotency, and monitor task completion.

```python
"""
AEGIS Runner - Batch Ingestion & Status Tracking Client
Requirements: pip install requests
"""

import hashlib
import json
import time
import uuid
import requests

AEGIS_API_URL = "http://localhost:8080/api/v1"
TENANT_ID = "tenant-billing-corp"


def generate_payload_hash(payload: dict) -> str:
    """Computes a deterministic SHA-256 hash of the payload for client-side auditing."""
    canonical_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def submit_batch_jobs(items: list[dict]) -> list[str]:
    """Submits a batch of jobs with deterministic idempotency keys."""
    job_ids = []
    
    print(f"Submitting batch of {len(items)} jobs for tenant: {TENANT_ID}")
    
    for item in items:
        # Deterministic or unique idempotency key per invoice/task
        idempotency_key = f"inv-{item['customer_id']}-{item['billing_cycle']}"
        
        headers = {
            "Content-Type": "application/json",
            "X-Tenant-Id": TENANT_ID,
            "X-Idempotency-Key": idempotency_key,
        }
        
        payload = {
            "action": "generate_invoice",
            "customer_id": item["customer_id"],
            "amount": item["amount"],
            "currency": "USD",
            "billing_cycle": item["billing_cycle"]
        }
        
        response = requests.post(
            f"{AEGIS_API_URL}/jobs",
            headers=headers,
            json=payload,
            timeout=5.0
        )
        
        if response.status_code in (200, 202):
            data = response.json()
            job_id = data.get("job_id")
            job_ids.append(job_id)
            print(f"  [ACCEPTED] Job ID: {job_id} | Key: {idempotency_key}")
        elif response.status_code == 409:
            print(f"  [CONFLICT] Idempotency key {idempotency_key} collided with different payload.")
        else:
            print(f"  [ERROR] Failed to submit: {response.status_code} - {response.text}")
            
    return job_ids


def poll_job_statuses(job_ids: list[str], max_wait_seconds: int = 30):
    """Polls the status of submitted jobs until all are COMPLETED or FAILED."""
    print("\nTracking execution progress...")
    start_time = time.time()
    
    pending_ids = set(job_ids)
    
    while pending_ids and (time.time() - start_time) < max_wait_seconds:
        finished_in_this_pass = set()
        
        for job_id in pending_ids:
            response = requests.get(
                f"{AEGIS_API_URL}/jobs/{job_id}",
                headers={"X-Tenant-Id": TENANT_ID},
                timeout=5.0
            )
            
            if response.status_code == 200:
                job_data = response.json()
                status = job_data.get("status")
                attempts = job_data.get("attempts", 0)
                
                if status in ("COMPLETED", "FAILED"):
                    print(f"  Job {job_id} reached terminal state: {status} (attempts: {attempts})")
                    finished_in_this_pass.add(job_id)
                    
        pending_ids -= finished_in_this_pass
        if pending_ids:
            time.sleep(2.0)
            
    if not pending_ids:
        print("\nAll batch jobs processed successfully.")
    else:
        print(f"\nTimed out. Remaining jobs in progress: {len(pending_ids)}")


if __name__ == "__main__":
    # Sample batch workload: 5 customer invoices
    sample_batch = [
        {"customer_id": f"cust_{i:04d}", "amount": 99.0 + i, "billing_cycle": "2026-10"}
        for i in range(1, 6)
    ]
    
    # 1. Enqueue batch
    submitted_ids = submit_batch_jobs(sample_batch)
    
    # 2. Monitor execution
    if submitted_ids:
        poll_job_statuses(submitted_ids)
```

---

## 6. Multi-Tenant Isolation: How It Works in AEGIS

Multi-tenancy means multiple independent customers or organizations share the same underlying computing and database infrastructure without seeing each other's data, corrupting each other's state, or monopolizing shared worker resources.

AEGIS enforces tenant isolation across four distinct layers:

```
[Incoming Request]
        │
        ▼
Layer 1: Ingestion Gate (Header Extraction & Tenant Verification)
        │
        ▼
Layer 2: Database Storage Segregation (Row-Level Multi-Tenancy & Compound Keys)
        │
        ▼
Layer 3: Deficit Round-Robin (DRR) Fair Scheduler (Anti-Starvation Algorithm)
        │
        ▼
Layer 4: Tenant Concurrency Quotas (Worker Pool Throttling)
```

### Layer 1: Ingestion Gate & Header Identification
* Every incoming API request must provide an `X-Tenant-Id` header (e.g., `tenant-alpha`).
* The API gateway validates tenant active status before accepting any payload into the queue.
* Requests missing a tenant header or referencing suspended tenants are rejected at the edge with HTTP `400 Bad Request` or `403 Forbidden`.

### Layer 2: Relational Data Segregation
* **Database Row Isolation:** Every record in the `jobs` table has a `tenant_id` foreign key referencing the `tenants` table.
* **Query Scoping:** Every status check, cancellation, or metric query strictly includes `WHERE tenant_id = $1`. Tenant A can never inspect, modify, or poll jobs belonging to Tenant B.
* **Compound Idempotency Keys:** The table `job_idempotency_keys` uses a compound primary key:
  ```sql
  PRIMARY KEY (tenant_id, key)
  ```
  This guarantees that if Tenant A uses key `invoice-001` and Tenant B also uses key `invoice-001`, they do not collide or interfere with one another.

### Layer 3: Deficit Round-Robin (DRR) Fair Scheduling (Eliminating Noisy Neighbors)
* **The Noisy Neighbor Problem:** If Tenant A submits 10,000 batch jobs at 09:00 AM, in a standard FIFO (First-In, First-Out) queue, Tenant B submitting 1 urgent password-reset email at 09:01 AM will wait 30 minutes until all 10,000 jobs finish.
* **The DRR Solution:**
  1. Each tenant is assigned a credit allowance per scheduling round (called `quantum`, e.g., 10 credits).
  2. The scheduler maintains an in-memory `deficit_counter` for each active tenant.
  3. On each scheduling round, every active tenant receives their quantum: `deficit += quantum`.
  4. The scheduler iterates through tenants:
     - If Tenant A has a deficit $\ge$ task cost, it executes a task for Tenant A and subtracts the cost from Tenant A's deficit.
     - The scheduler immediately moves to Tenant B, granting its turn.
  5. Result: Tenant A's 10,000 jobs are throttled across multiple rounds, while Tenant B's urgent job executes immediately in the very next round.

### Layer 4: Per-Tenant Concurrency Quotas
* In addition to scheduling fairness, each tenant has a maximum active concurrency limit configured in the `tenants` table (e.g., `concurrency_limit = 5`).
* Even if 20 worker routines are idle, a single tenant can only consume up to its assigned limit. The remaining worker slots remain reserved for other tenants.

---

## 7. Architectural Comparison: AWS ECS Fargate vs. Azure Container Apps

A common question is: *"Why not simply run this container on AWS ECS Fargate? What is the complexity difference?"*

While running a container in AWS Fargate sounds straightforward in theory, setting up production-grade, secure, multi-tenant background orchestration with live streaming and database-driven autoscaling on AWS introduces significant infrastructure sprawl and hidden costs.

Below is the technical evaluation of AWS ECS Fargate versus Azure Container Apps (ACA) for the AEGIS Runner domain:

```
AWS ECS Architecture (Fragmented, Multiple Resources Required)
Internet ──> Application Load Balancer ──> ECS Fargate Container (Private Subnet)
                    │                                │
                    ▼                                ▼
            ALB Target Group                 NAT Gateway (~$32/mo)
                    │                                │
                    ▼                                ▼
            Idle Timeout Reconnects           Internet Egress
                                                     │
                                                     ▼
                                            Amazon RDS PostgreSQL

---------------------------------------------------------------------------------

Azure Container Apps Architecture (Clean PaaS Monolith)
Internet ──> ACA Built-in Envoy Ingress ──> AEGIS Runner Container (Single Resource)
             (Free TLS, Native HTTP/2 SSE)            │
                                                     ▼
                                            Direct Outbound & Private Link
                                                     │
                                                     ▼
                                            Azure Database for PostgreSQL
```

### Direct Feature Comparison

| Architectural Vector | AWS ECS Fargate Approach | Azure Container Apps Approach (Selected) | Why Azure is Better for AEGIS |
| :--- | :--- | :--- | :--- |
| **Public Ingress & TLS** | Requires provisioning an Application Load Balancer (ALB), Target Groups, ALB Listeners, and ACM SSL Certificates. | Built-in Envoy ingress with automated, free public HTTPS certificates out of the box with one line of config. | Zero ALB infrastructure to manage, configure, or pay for. |
| **Real-Time Streaming (SSE)** | ALB defaults to a 60-second idle timeout. Long-lived Server-Sent Events drop frequently unless ALB timeouts are tuned and client reconnect renegotiations are engineered. | Native HTTP/2 event streaming support via built-in Envoy ingress with zero drop renegotiations. | Flawless, zero-config real-time dashboard updates without socket dropouts. |
| **Egress Networking & Baseline Cost** | Fargate containers connecting to RDS in private subnets require an **AWS NAT Gateway** for outbound internet traffic. A single NAT Gateway costs **~$32/month flat baseline** plus $0.045/GB data processing, even when idle. | Built-in secure outbound routing. No NAT Gateways required for basic container egress. | Eliminates unnecessary fixed cloud overhead for small and medium deployments. |
| **Queue-Depth Autoscaling (KEDA)** | ECS Auto Scaling only scales on CPU and RAM. To scale based on PostgreSQL queue depth, you must build custom CloudWatch metrics + Lambda, or deploy full Kubernetes (EKS) + install KEDA. | **KEDA is natively integrated** directly into Azure Container Apps at no extra cost. ACA can scale container replicas directly from PostgreSQL queries. | Native queue-driven autoscaling without deploying Kubernetes clusters. |
| **Infrastructure as Code (IaC) Footprint** | Demands 12 to 15 Terraform resources: VPC, Public Subnets, Private Subnets, Internet Gateway, NAT Gateway, EIP, ALB, Target Group, Listener, ECS Cluster, Task Definition, ECS Service, IAM Roles. | Exactly **2 primary Terraform resources**: 1 `azurerm_container_app` + 1 `azurerm_postgresql_flexible_server`. | Massively reduced operational surface area, faster deployment, and zero configuration drift. |
| **Hackathon Alignment** | Outside track focus. | Core target of the Microsoft Azure Cloud-Native Infrastructure & Resilient Systems track. | Directly fulfills the evaluation track criteria using native Azure PaaS features. |

### Summary
On AWS, achieving what AEGIS requires (public HTTPS ingress, long-lived SSE streaming, queue-depth autoscaling, and private database connectivity) requires stitching together an ALB, ECS, NAT Gateway, CloudWatch, and multiple IAM roles. 

On Azure, **Azure Container Apps gives you the container runtime, Envoy ingress, free TLS, long-lived HTTP/2 streaming, and KEDA autoscaling in a single PaaS resource** backed directly by Azure Database for PostgreSQL.
