const fmtTime = (s) => s ? new Date(s).toLocaleTimeString([], { hour12: false }) : "—";
const fmtConf = (v) => v == null ? "—" : v.toFixed(2);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

async function fetchJSON(url, opts = {}) {
  const r = await fetch(url, { credentials: "same-origin", ...opts });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

function renderTable(sel, rows, mapper) {
  const tbody = document.querySelector(`${sel} tbody`);
  tbody.innerHTML = rows.length ? rows.map(mapper).join("") : `<tr><td colspan="99" class="sub">no rows</td></tr>`;
}

async function refreshOverview() {
  const d = await fetchJSON("/api/overview");
  document.getElementById("metrics").innerHTML = `
    <div class="metric"><span class="v">${d.raw_count}</span><span class="k">raw</span></div>
    <div class="metric"><span class="v">${d.processed_count}</span><span class="k">processed</span></div>
    <div class="metric"><span class="v">${d.raw_count - d.processed_count}</span><span class="k">in flight</span></div>
  `;
  renderTable("#by-label", d.by_label, r => `<tr><td><span class="pill">${esc(r.label)}</span></td><td>${r.n}</td><td class="sub">${fmtConf(r.avg_conf)}</td></tr>`);
  renderTable("#by-priority", d.by_priority, r => `<tr><td><span class="pill ${esc(r.priority)}">${esc(r.priority)}</span></td><td>${r.n}</td></tr>`);
  renderTable("#pubsub", d.pubsub, r => `<tr><td class="sub">${esc(r.topic)}</td><td>${esc(r.state)}</td><td>${r.count}</td></tr>`);
}

async function refreshMessages() {
  const d = await fetchJSON("/api/messages?limit=30");
  renderTable("#messages", d.rows, r => `
    <tr>
      <td class="sub">${fmtTime(r.received_at)}</td>
      <td class="sub">${esc(r.sender)}</td>
      <td>${esc(r.subject)}</td>
      <td>${r.predicted_label ? `<span class="pill">${esc(r.predicted_label)}</span>` : `<span class="sub">pending</span>`}</td>
      <td>${r.priority ? `<span class="pill ${esc(r.priority)}">${esc(r.priority)}</span>` : ""}</td>
      <td class="sub">${fmtConf(r.confidence)}</td>
      <td class="sub">${esc((r.summary || "").slice(0, 80))}</td>
    </tr>
  `);
}

async function refreshDLQ() {
  const d = await fetchJSON("/api/dlq");
  renderTable("#dlq", d.rows, r => `
    <tr>
      <td class="sub">${fmtTime(r.published_at)}</td>
      <td class="sub">${esc(r.topic)}</td>
      <td>${r.delivery_count}</td>
      <td class="sub">${esc((r.last_error || "").slice(0, 120))}</td>
    </tr>
  `);
}

async function refreshRuns() {
  const d = await fetchJSON("/api/runs");
  renderTable("#runs", d.rows, r => `
    <tr>
      <td>${esc(r.kind)}</td>
      <td><span class="pill status-${esc(r.status)}">${esc(r.status)}</span></td>
      <td class="sub">${fmtTime(r.started_at)}</td>
      <td class="sub">${esc((r.details || "").slice(0, 80))}</td>
    </tr>
  `);
}

async function refreshEval() {
  const d = await fetchJSON("/api/eval");
  renderTable("#eval", d.rows, r => `
    <tr>
      <td class="sub">${esc(r.run_id.slice(0, 8))}</td>
      <td><span class="pill">${esc(r.label)}</span></td>
      <td>${r.precision.toFixed(2)}</td>
      <td>${r.recall.toFixed(2)}</td>
      <td>${r.f1.toFixed(2)}</td>
      <td>${r.support}</td>
      <td class="sub">${fmtTime(r.started_at)}</td>
    </tr>
  `);
}

async function refreshSlack() {
  const d = await fetchJSON("/api/slack");
  renderTable("#slack", d.rows, r => `
    <tr>
      <td class="sub">${fmtTime(r.ts)}</td>
      <td class="sub">${esc(r.channel)}</td>
      <td>${esc(r.text)}</td>
    </tr>
  `);
}

const REFRESHERS = {
  overview: refreshOverview,
  messages: refreshMessages,
  dlq: refreshDLQ,
  runs: refreshRuns,
  eval: refreshEval,
  slack: refreshSlack,
};

async function refresh(name) {
  try { await REFRESHERS[name](); }
  catch (e) { console.error(name, e); }
}

async function refreshAll() {
  await Promise.all(Object.keys(REFRESHERS).map(refresh));
}

document.querySelectorAll("[data-action]").forEach(btn => {
  btn.addEventListener("click", async () => {
    const action = btn.dataset.action;
    const out = document.getElementById("action-output");
    out.textContent = `running ${action}…`;
    btn.disabled = true;
    try {
      const r = await fetchJSON(`/api/actions/${action}`, { method: "POST" });
      out.textContent = JSON.stringify(r, null, 2);
      await refreshAll();
    } catch (e) {
      out.textContent = `error: ${e.message}`;
    } finally {
      btn.disabled = false;
    }
  });
});

refreshAll();
setInterval(refreshAll, 5000);
