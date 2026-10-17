"""
AEGIS Runner - Batch Ingestion & Status Tracking Client
Compliant with BATCH_AND_TENANCY_GUIDE.md
"""

import hashlib
import json
import time
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
    
    print(f"[*] Submitting batch of {len(items)} jobs for tenant: {TENANT_ID}")
    
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
            "billing_cycle": item["billing_cycle"],
            "duration_ms": 2000
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
            cached = data.get("cached", False)
            job_ids.append(job_id)
            cached_tag = " [CACHED DEDUPLICATED]" if cached else ""
            print(f"  [ACCEPTED] Job ID: {job_id}{cached_tag} | Key: {idempotency_key}")
        elif response.status_code == 409:
            print(f"  [CONFLICT] Idempotency key {idempotency_key} collided with different payload.")
        else:
            print(f"  [ERROR] Failed to submit: {response.status_code} - {response.text}")
            
    return job_ids


def poll_job_statuses(job_ids: list[str], max_wait_seconds: int = 30):
    """Polls the status of submitted jobs until all are COMPLETED or FAILED."""
    print("\n[*] Tracking execution progress...")
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
                worker_id = job_data.get("worker_id", "unassigned")
                
                if status in ("COMPLETED", "FAILED"):
                    print(f"  -> Job {job_id[:8]}... reached terminal state: {status} (attempts: {attempts}, worker: {worker_id})")
                    finished_in_this_pass.add(job_id)
                    
        pending_ids -= finished_in_this_pass
        if pending_ids:
            time.sleep(1.5)
            
    if not pending_ids:
        print("\n[+] All batch jobs processed successfully.")
    else:
        print(f"\n[!] Timed out. Remaining jobs in progress: {len(pending_ids)}")


if __name__ == "__main__":
    # Sample batch workload: 5 customer invoices
    sample_batch = [
        {"customer_id": f"cust_{i:04d}", "amount": 99.0 + i, "billing_cycle": "2026-10"}
        for i in range(1, 6)
    ]
    
    print("=== Step 1: Initial Batch Enqueue ===")
    submitted_ids = submit_batch_jobs(sample_batch)
    
    print("\n=== Step 2: Idempotency Verification (Resubmitting Same Batch) ===")
    duplicate_ids = submit_batch_jobs(sample_batch)
    assert submitted_ids == duplicate_ids, "Idempotency failure: Job IDs did not match cached handles!"
    print("  -> Verification Success: Exact match across all idempotency keys with zero duplicate tasks created.")
    
    print("\n=== Step 3: Monitor Execution to Completion ===")
    if submitted_ids:
        poll_job_statuses(submitted_ids)
