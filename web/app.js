// AEGIS Runner - Technical Observability & Control Frontend
// Adheres strictly to zero-emoji and clean geometric indicator design rules.

(function () {
  'use strict';

  const API_BASE = '/api/v1';
  let eventSource = null;
  let simulatedLatencyActive = false;

  // DOM Elements
  const statIngested = document.getElementById('stat-ingested');
  const statPending = document.getElementById('stat-pending');
  const statRunning = document.getElementById('stat-running');
  const statCompleted = document.getElementById('stat-completed');
  const statWorkers = document.getElementById('stat-workers');
  const statRecovered = document.getElementById('stat-recovered');
  const statDropped = document.getElementById('stat-dropped');

  const sseStatusDot = document.getElementById('sse-status-dot');
  const sseStatusText = document.getElementById('sse-status-text');

  const eventStreamContainer = document.getElementById('event-stream-container');
  const tenantList = document.getElementById('tenant-list');
  const jobsTableContainer = document.getElementById('jobs-table-container');

  const taskForm = document.getElementById('task-form');
  const formTenant = document.getElementById('form-tenant');
  const formAction = document.getElementById('form-action');
  const formIdempotency = document.getElementById('form-idempotency');
  const formPayload = document.getElementById('form-payload');

  const btnFloodBatch = document.getElementById('btn-flood-batch');
  const btnPriorityTask = document.getElementById('btn-priority-task');
  const btnTestIdempotency = document.getElementById('btn-test-idempotency');
  const btnKillWorker = document.getElementById('btn-kill-worker');
  const btnToggleLatency = document.getElementById('btn-toggle-latency');
  const latencyIndicator = document.getElementById('latency-indicator');
  const chaosAlertBox = document.getElementById('chaos-alert-box');
  const btnClearStream = document.getElementById('btn-clear-stream');
  const btnRefreshJobs = document.getElementById('btn-refresh-jobs');

  // Modal elements
  const jobModal = document.getElementById('job-modal');
  const modalClose = document.getElementById('modal-close');
  const modalJobID = document.getElementById('modal-job-id');
  const modalTenant = document.getElementById('modal-tenant');
  const modalStatus = document.getElementById('modal-status');
  const modalAttempts = document.getElementById('modal-attempts');
  const modalWorker = document.getElementById('modal-worker');
  const modalPayload = document.getElementById('modal-payload');
  const modalLogs = document.getElementById('modal-logs');

  // --- SSE Event Stream ---
  function connectSSE() {
    if (eventSource) {
      eventSource.close();
    }

    eventSource = new EventSource(`${API_BASE}/stream/events`);

    eventSource.onopen = function () {
      sseStatusDot.className = 'inline-block w-2 h-2 rounded-full bg-emerald-500';
      sseStatusText.textContent = 'STREAM: ACTIVE';
    };

    eventSource.onerror = function () {
      sseStatusDot.className = 'inline-block w-2 h-2 rounded-full bg-rose-500';
      sseStatusText.textContent = 'STREAM: DISCONNECTED';
      setTimeout(connectSSE, 3000);
    };

    const handleEvent = function (event) {
      try {
        const data = JSON.parse(event.data);
        renderStreamEntry(data);
        refreshStats();
        if (data.event === 'JOB_COMPLETED' || data.event === 'JOB_STARTED' || data.event === 'REAPER_RECOVERED') {
          refreshTenants();
          refreshJobs();
        }
      } catch (e) {
        console.error('Error parsing SSE event:', e);
      }
    };

    const eventsList = [
      'JOB_ENQUEUED', 'JOB_SCHEDULED', 'JOB_STARTED', 'HEARTBEAT_TICK',
      'JOB_COMPLETED', 'JOB_FAILED', 'WORKER_KILLED', 'REAPER_RECOVERED'
    ];
    eventsList.forEach(evt => eventSource.addEventListener(evt, handleEvent));
  }

  function getBadgeHTML(eventType) {
    switch (eventType) {
      case 'JOB_ENQUEUED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-zinc-800 text-zinc-300 border border-zinc-700">ENQUEUED</span>';
      case 'JOB_SCHEDULED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-blue-500/10 text-blue-400 border border-blue-500/20">SCHEDULED</span>';
      case 'JOB_STARTED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">RUNNING</span>';
      case 'HEARTBEAT_TICK':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">HEARTBEAT</span>';
      case 'JOB_COMPLETED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">COMPLETED</span>';
      case 'WORKER_KILLED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-rose-500/20 text-rose-400 border border-rose-500/30">KILLED</span>';
      case 'REAPER_RECOVERED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-purple-500/20 text-purple-300 border border-purple-500/30">RECOVERED</span>';
      case 'JOB_FAILED':
        return '<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-rose-500/10 text-rose-400 border border-rose-500/20">FAILED</span>';
      default:
        return `<span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-zinc-800 text-zinc-400">${eventType}</span>`;
    }
  }

  function renderStreamEntry(data) {
    const timeStr = data.timestamp ? data.timestamp.split('T')[1].replace('Z', '').slice(0, 12) : new Date().toISOString().slice(11, 23);
    const row = document.createElement('div');
    row.className = 'p-2 bg-zinc-950/80 hover:bg-zinc-850 border border-zinc-850 hover:border-zinc-700 rounded transition-colors flex items-start justify-between cursor-pointer';
    
    if (data.job_id) {
      row.onclick = () => openJobModal(data.job_id);
    }

    const shortID = data.job_id ? data.job_id.slice(0, 8) : '--------';
    const tenantBadge = data.tenant_id ? `<span class="text-zinc-500">[${data.tenant_id}]</span>` : '';

    row.innerHTML = `
      <div class="flex items-start space-x-2 truncate">
        <span class="text-zinc-500 text-[11px]">${timeStr}</span>
        ${getBadgeHTML(data.event)}
        ${tenantBadge}
        <span class="text-zinc-300 truncate">${escapeHTML(data.message || '')}</span>
      </div>
      <div class="text-zinc-600 hover:text-zinc-400 text-[11px] font-mono ml-2 shrink-0">${shortID}</div>
    `;

    eventStreamContainer.insertBefore(row, eventStreamContainer.firstChild);

    // Keep max 200 items in buffer
    if (eventStreamContainer.children.length > 200) {
      eventStreamContainer.removeChild(eventStreamContainer.lastChild);
    }
  }

  function escapeHTML(str) {
    return str.replace(/[&<>'"]/g, tag => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[tag] || tag));
  }

  // --- Stats & Tenant Fetching ---
  async function refreshStats() {
    try {
      const res = await fetch(`${API_BASE}/stats`);
      if (!res.ok) return;
      const data = await res.json();

      statIngested.textContent = data.total_ingested;
      statPending.textContent = data.pending;
      statRunning.textContent = data.running;
      statCompleted.textContent = data.completed;
      statWorkers.textContent = `${data.active_workers} / ${data.concurrency}`;
      statRecovered.textContent = data.recovered;
      statDropped.textContent = data.dropped;
    } catch (e) {
      console.error('Error fetching stats:', e);
    }
  }

  async function refreshTenants() {
    try {
      const res = await fetch(`${API_BASE}/tenants`);
      if (!res.ok) return;
      const tenants = await res.json();

      tenantList.innerHTML = '';
      tenants.forEach(t => {
        const card = document.createElement('div');
        card.className = 'p-3 bg-zinc-950 border border-zinc-800 rounded space-y-2';

        const concurrencyPct = Math.min(100, Math.round((t.active_running / t.concurrency_limit) * 100));

        card.innerHTML = `
          <div class="flex items-center justify-between">
            <div>
              <span class="font-mono text-xs font-semibold text-zinc-200">${t.id}</span>
              <span class="text-zinc-500 text-[11px] block">${t.name}</span>
            </div>
            <div class="text-right font-mono text-[11px]">
              <span class="text-zinc-400">DEFICIT: </span>
              <span class="text-cyan-400 font-semibold">${t.deficit} cr</span>
            </div>
          </div>

          <div class="space-y-1">
            <div class="flex justify-between text-[11px] font-mono text-zinc-500">
              <span>Active Slots: ${t.active_running} / ${t.concurrency_limit}</span>
              <span>Pending: ${t.pending_count} | Done: ${t.completed_count}</span>
            </div>
            <div class="w-full bg-zinc-800 h-1.5 rounded-full overflow-hidden">
              <div class="bg-emerald-500 h-full rounded-full transition-all duration-300" style="width: ${concurrencyPct}%"></div>
            </div>
          </div>
        `;
        tenantList.appendChild(card);
      });
    } catch (e) {
      console.error('Error fetching tenants:', e);
    }
  }

  async function refreshJobs() {
    try {
      const res = await fetch(`${API_BASE}/jobs?limit=25`);
      if (!res.ok) return;
      const jobs = await res.json();

      jobsTableContainer.innerHTML = '';
      if (jobs.length === 0) {
        jobsTableContainer.innerHTML = '<div class="text-zinc-600 font-mono text-xs p-2">No jobs ingested yet.</div>';
        return;
      }

      jobs.forEach(job => {
        const item = document.createElement('div');
        item.className = 'p-2.5 bg-zinc-950 border border-zinc-800/80 hover:border-zinc-700 rounded transition-colors flex items-center justify-between cursor-pointer';
        item.onclick = () => openJobModal(job.id);

        let statusBadge = '';
        if (job.status === 'RUNNING') {
          statusBadge = '<span class="text-emerald-400 font-medium">RUNNING</span>';
        } else if (job.status === 'COMPLETED') {
          statusBadge = '<span class="text-zinc-400">COMPLETED</span>';
        } else if (job.status === 'FAILED') {
          statusBadge = '<span class="text-rose-400 font-medium">FAILED</span>';
        } else {
          statusBadge = '<span class="text-zinc-500">PENDING</span>';
        }

        const workerBadge = job.worker_id ? `<span class="text-zinc-500">[${job.worker_id}]</span>` : '';

        item.innerHTML = `
          <div class="flex items-center space-x-2 truncate">
            <span class="text-zinc-300 font-semibold truncate">${job.id.slice(0, 8)}</span>
            <span class="text-zinc-500">${job.tenant_id}</span>
            ${workerBadge}
          </div>
          <div class="flex items-center space-x-3 shrink-0">
            <span class="text-[11px] font-mono text-zinc-500">att: ${job.attempts}</span>
            <span class="text-xs font-mono">${statusBadge}</span>
          </div>
        `;
        jobsTableContainer.appendChild(item);
      });
    } catch (e) {
      console.error('Error fetching jobs:', e);
    }
  }

  // --- Job Modal Details ---
  async function openJobModal(jobID) {
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobID}`);
      if (!res.ok) return;
      const job = await res.json();

      modalJobID.textContent = job.job_id;
      modalTenant.textContent = job.tenant_id;
      modalStatus.textContent = job.status;
      modalAttempts.textContent = `${job.attempts} / ${job.max_attempts}`;
      modalWorker.textContent = job.worker_id || 'None';
      modalPayload.textContent = JSON.stringify(job.payload, null, 2);

      modalLogs.innerHTML = '';
      if (job.logs && job.logs.length > 0) {
        job.logs.forEach(l => {
          const entry = document.createElement('div');
          const time = l.created_at ? l.created_at.slice(11, 19) : '';
          entry.innerHTML = `<span class="text-zinc-600">[${time}]</span> <span class="text-zinc-300 font-semibold">${l.event}:</span> ${escapeHTML(l.message)}`;
          modalLogs.appendChild(entry);
        });
      } else {
        modalLogs.innerHTML = '<span class="text-zinc-600">No execution logs recorded yet.</span>';
      }

      jobModal.classList.remove('hidden');
    } catch (e) {
      console.error('Error opening job modal:', e);
    }
  }

  modalClose.onclick = () => jobModal.classList.add('hidden');
  window.onkeydown = (e) => {
    if (e.key === 'Escape') jobModal.classList.add('hidden');
  };

  // --- Task Submission ---
  taskForm.onsubmit = async function (e) {
    e.preventDefault();
    let payload = {};
    try {
      payload = JSON.parse(formPayload.value);
    } catch (err) {
      alert('Invalid JSON in payload field: ' + err.message);
      return;
    }

    const tenantID = formTenant.value;
    const action = formAction.value.trim();
    const idempotencyKey = formIdempotency.value.trim();

    const headers = { 'Content-Type': 'application/json', 'X-Tenant-Id': tenantID };
    if (idempotencyKey) {
      headers['X-Idempotency-Key'] = idempotencyKey;
    }

    try {
      const res = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({ tenant_id: tenantID, action: action, payload: payload })
      });
      const data = await res.json();
      if (res.status === 200 && data.cached) {
        showChaosAlert(`[IDEMPOTENCY HIT] Returned existing handle ${data.job_id.slice(0, 8)} (${data.status})`);
      } else if (res.status === 409) {
        showChaosAlert(`[409 CONFLICT] Idempotency key collision with different payload!`);
      }
      refreshStats();
      refreshJobs();
    } catch (err) {
      console.error('Submission error:', err);
    }
  };

  // --- Preset Workload Triggers ---
  btnFloodBatch.onclick = async function () {
    try {
      const res = await fetch(`${API_BASE}/chaos/flood`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant_id: 'tenant-alpha', count: 200 })
      });
      const data = await res.json();
      if (!res.ok) {
        showChaosAlert(`[CHAOS ERROR] ${data.error || 'Failed to trigger flood'}`);
        return;
      }
      showChaosAlert(`Flood enqueued: 200 batch tasks for tenant-alpha`);
      refreshStats();
      refreshTenants();
    } catch (err) {
      console.error('Flood error:', err);
      showChaosAlert(`[CHAOS ERROR] Network failure triggering flood`);
    }
  };

  btnPriorityTask.onclick = async function () {
    try {
      const res = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Tenant-Id': 'tenant-beta'
        },
        body: JSON.stringify({
          tenant_id: 'tenant-beta',
          action: 'send_urgent_security_alert',
          payload: { user_id: 101, priority: 'URGENT', duration_ms: 2000 }
        })
      });
      const data = await res.json();
      showChaosAlert(`Priority job ${data.job_id.slice(0, 8)} submitted for tenant-beta!`);
      refreshStats();
      refreshTenants();
    } catch (err) {
      console.error('Priority task error:', err);
    }
  };

  let testKeyCounter = 1;
  btnTestIdempotency.onclick = async function () {
    const key = `test-idemp-${testKeyCounter}`;
    formIdempotency.value = key;

    // Send first request
    const payload = { test: true, attempt: 1, duration_ms: 3000 };
    showChaosAlert(`Sending 1st request with key '${key}'...`);

    const res1 = await fetch(`${API_BASE}/jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-Id': 'tenant-beta',
        'X-Idempotency-Key': key
      },
      body: JSON.stringify({ payload })
    });
    const data1 = await res1.json();

    // Immediately send second identical request
    setTimeout(async () => {
      showChaosAlert(`Sending 2nd identical retry with key '${key}'...`);
      const res2 = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Tenant-Id': 'tenant-beta',
          'X-Idempotency-Key': key
        },
        body: JSON.stringify({ payload })
      });
      const data2 = await res2.json();
      if (data2.cached && data2.job_id === data1.job_id) {
        showChaosAlert(`[IDEMPOTENCY PROVEN] Retry returned same Job ID ${data2.job_id.slice(0, 8)} without duplicate enqueue!`);
      }
      testKeyCounter++;
    }, 400);
  };

  // --- Chaos Controls ---
  btnKillWorker.onclick = async function () {
    try {
      const res = await fetch(`${API_BASE}/chaos/kill-worker`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await res.json();
      if (res.ok) {
        showChaosAlert(`[KILL INJECTED] Aborted worker ${data.worker_id} on job ${data.job_id.slice(0, 8)}. Lease left intact for Reaper!`);
      } else {
        showChaosAlert(`[CHAOS ERROR] ${data.error || 'No active worker currently running'}`);
      }
      refreshStats();
    } catch (err) {
      console.error('Kill worker error:', err);
      showChaosAlert(`[CHAOS ERROR] Network failure triggering worker kill`);
    }
  };

  btnToggleLatency.onclick = async function () {
    simulatedLatencyActive = !simulatedLatencyActive;
    const delay = simulatedLatencyActive ? 500 : 0;
    try {
      const res = await fetch(`${API_BASE}/chaos/latency`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ delay_ms: delay })
      });
      const data = await res.json();
      if (!res.ok) {
        simulatedLatencyActive = !simulatedLatencyActive; // revert state
        showChaosAlert(`[CHAOS ERROR] ${data.error || 'Failed to toggle latency'}`);
        return;
      }
      latencyIndicator.textContent = simulatedLatencyActive ? '500MS' : 'OFF';
      latencyIndicator.className = simulatedLatencyActive ? 'font-mono text-xs text-amber-400 font-semibold' : 'font-mono text-xs text-zinc-500';
      showChaosAlert(`Simulated database latency set to ${delay}ms`);
    } catch (err) {
      simulatedLatencyActive = !simulatedLatencyActive; // revert state
      console.error('Latency error:', err);
      showChaosAlert(`[CHAOS ERROR] Network failure toggling latency`);
    }
  };

  function showChaosAlert(msg) {
    chaosAlertBox.textContent = msg;
    chaosAlertBox.classList.remove('hidden');
    setTimeout(() => {
      chaosAlertBox.classList.add('hidden');
    }, 7000);
  }

  btnClearStream.onclick = () => {
    eventStreamContainer.innerHTML = '';
  };
  btnRefreshJobs.onclick = () => {
    refreshJobs();
    refreshStats();
  };

  // Initialize
  connectSSE();
  refreshStats();
  refreshTenants();
  refreshJobs();

  // Periodic polling for counters & tenants
  setInterval(refreshStats, 2000);
  setInterval(refreshTenants, 3000);
  setInterval(refreshJobs, 4000);

})();
