const REFRESH_MS = 5000;
const SUMMARY_WINDOW_MIN = 5;
const TIMELINE_WINDOW_MIN = 15;

const $ = (id) => document.getElementById(id);

async function fetchJson(url) {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

function errRateClass(r) {
  if (r >= 0.1) return "err-rate-high";
  if (r >= 0.02) return "err-rate-mid";
  return "err-rate-low";
}

function pct(x) {
  return `${(x * 100).toFixed(2)}%`;
}

async function refreshHealth() {
  const el = $("health");
  try {
    const h = await fetchJson("/healthz");
    const ok = h.status === "ok";
    el.textContent = ok ? "healthy" : `degraded (ClickHouse: ${h.clickhouse})`;
    el.className = `health ${ok ? "ok" : "err"}`;
  } catch (e) {
    el.textContent = "API unreachable";
    el.className = "health err";
  }
}

async function refreshSummary() {
  $("window-label").textContent = SUMMARY_WINDOW_MIN;
  try {
    const s = await fetchJson(`/api/v1/analytics/summary?minutes=${SUMMARY_WINDOW_MIN}`);
    $("total-events").textContent = s.total_events.toLocaleString();
    const rateEl = $("error-rate");
    rateEl.textContent = pct(s.error_rate);
    rateEl.className = `v ${errRateClass(s.error_rate)}`;
    $("updated").textContent = new Date().toLocaleTimeString();

    const tbody = document.querySelector("#by-type tbody");
    tbody.innerHTML = "";
    for (const row of s.by_event_type) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.event_type}</td>
        <td>${row.events.toLocaleString()}</td>
        <td>${row.errors.toLocaleString()}</td>
        <td class="${errRateClass(row.error_rate)}">${pct(row.error_rate)}</td>
      `;
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("summary failed", e);
  }
}

async function refreshMinutes() {
  try {
    const rows = await fetchJson(`/api/v1/analytics/minute?minutes=${TIMELINE_WINDOW_MIN}`);
    const tbody = document.querySelector("#minutes tbody");
    tbody.innerHTML = "";
    for (const r of rows.slice(0, 200)) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.minute}</td>
        <td>${r.event_type}</td>
        <td class="status-${r.status}">${r.status}</td>
        <td>${r.events.toLocaleString()}</td>
        <td>${r.avg_latency_ms.toFixed(1)}</td>
        <td>${r.p95_latency_ms.toFixed(1)}</td>
      `;
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("minutes failed", e);
  }
}

async function tick() {
  await Promise.all([refreshHealth(), refreshSummary(), refreshMinutes()]);
}

tick();
setInterval(tick, REFRESH_MS);
