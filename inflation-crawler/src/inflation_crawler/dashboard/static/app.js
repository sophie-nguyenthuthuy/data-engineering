const $ = (s) => document.querySelector(s);
let chart;

async function j(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

async function loadCategories() {
  const cats = await j("/api/categories");
  const sel = $("#category");
  sel.innerHTML = '<option value="">(all)</option>';
  for (const c of cats) {
    const o = document.createElement("option");
    o.value = o.textContent = c;
    sel.appendChild(o);
  }
}

function alignSeries(crawled, cpi) {
  // Use union of periods, sorted.
  const periods = Array.from(
    new Set([...crawled.map((x) => x.period), ...cpi.map((x) => x.period)])
  ).sort();
  const crawledMap = new Map(crawled.map((x) => [x.period, x.monthly_inflation_pct]));
  const cpiMap = new Map(cpi.map((x) => [x.period, x.monthly_pct]));
  return {
    labels: periods,
    crawled: periods.map((p) => crawledMap.get(p) ?? null),
    cpi: periods.map((p) => cpiMap.get(p) ?? null),
  };
}

async function loadChart() {
  const cat = $("#category").value;
  const series = $("#series").value || "CUUR0000SA0";

  const inflation = await j("/api/inflation" + (cat ? `?category=${encodeURIComponent(cat)}` : ""));
  let cpi = [];
  try { cpi = await j(`/api/cpi?series_id=${encodeURIComponent(series)}`); }
  catch (e) { /* CPI may not be loaded yet */ }

  const aligned = alignSeries(inflation.series, cpi);

  if (chart) chart.destroy();
  chart = new Chart($("#chart"), {
    type: "line",
    data: {
      labels: aligned.labels,
      datasets: [
        {
          label: `Crawled ${cat || "all"} (MoM %)`,
          data: aligned.crawled,
          borderColor: "#58a6ff",
          backgroundColor: "rgba(88,166,255,0.15)",
          tension: 0.25,
          spanGaps: true,
        },
        {
          label: `CPI ${series} (MoM %)`,
          data: aligned.cpi,
          borderColor: "#f778ba",
          backgroundColor: "rgba(247,120,186,0.1)",
          tension: 0.25,
          spanGaps: true,
          borderDash: [6, 4],
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { labels: { color: "#e6edf3" } },
      },
      scales: {
        x: { ticks: { color: "#8b949e" }, grid: { color: "#21262d" } },
        y: { ticks: { color: "#8b949e", callback: (v) => v + "%" }, grid: { color: "#21262d" } },
      },
    },
  });

  const badge = $("#rate-badge");
  if (inflation.annualized_pct != null) {
    const sign = inflation.annualized_pct >= 0 ? "+" : "";
    badge.textContent = `annualized: ${sign}${inflation.annualized_pct.toFixed(2)}%`;
  } else {
    badge.textContent = "no data";
  }
}

async function loadProducts() {
  const cat = $("#category").value;
  const rows = await j("/api/products?limit=25" + (cat ? `&category=${encodeURIComponent(cat)}` : ""));
  const body = $("#products tbody");
  body.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.title ?? ""}</td>
      <td>${r.brand ?? ""}</td>
      <td>${r.category ?? ""}</td>
      <td>${r.currency} ${Number(r.price).toFixed(2)}</td>
      <td>${r.source}</td>
      <td>${String(r.fetch_time).slice(0, 10)}</td>
    `;
    body.appendChild(tr);
  }
}

async function refresh() {
  try {
    await loadChart();
    await loadProducts();
  } catch (e) {
    console.error(e);
  }
}

$("#refresh").addEventListener("click", refresh);
$("#category").addEventListener("change", refresh);

(async () => {
  await loadCategories();
  await refresh();
})();
