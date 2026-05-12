"""HTML report generator with SVG timeline visualization.

Produces a self-contained HTML file showing:
  - Test configuration and summary (pass/fail badge)
  - Per-chaos-type fault timeline
  - SVG operation timeline (swim lanes per process)
  - Linearization order (if valid) or first anomaly description
  - Raw history as a collapsible JSON block
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from jinja2 import Template

from ..core.history import Entry, Op
from ..core.checker import CheckResult


TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Jepsen Linearizability Report</title>
<style>
  :root {
    --ok: #22c55e; --fail: #ef4444; --info: #f59e0b;
    --invoke: #3b82f6; --bg: #0f172a; --surface: #1e293b;
    --text: #e2e8f0; --muted: #94a3b8; --border: #334155;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'JetBrains Mono', 'Fira Code', monospace; background: var(--bg); color: var(--text); padding: 2rem; }
  h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
  h2 { font-size: 1.1rem; color: var(--muted); margin: 1.5rem 0 0.75rem; border-bottom: 1px solid var(--border); padding-bottom: 0.25rem; }
  .badge { display: inline-block; padding: 0.25rem 0.75rem; border-radius: 9999px; font-weight: 700; font-size: 0.9rem; }
  .pass { background: var(--ok); color: #fff; }
  .fail { background: var(--fail); color: #fff; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1rem; margin: 1rem 0; }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; }
  .stat-val { font-size: 1.8rem; font-weight: 700; }
  .stat-lbl { font-size: 0.75rem; color: var(--muted); margin-top: 0.25rem; }
  .timeline-wrap { overflow-x: auto; background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; }
  svg text { font-family: inherit; font-size: 11px; fill: var(--text); }
  details { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; margin-top: 1rem; }
  summary { padding: 0.75rem 1rem; cursor: pointer; font-weight: 600; }
  pre { padding: 1rem; overflow-x: auto; font-size: 0.75rem; color: var(--muted); max-height: 400px; overflow-y: auto; }
  .fault-list { list-style: none; }
  .fault-list li { padding: 0.25rem 0; color: var(--muted); font-size: 0.85rem; }
  .fault-list li span { color: var(--fail); font-weight: 600; }
</style>
</head>
<body>
<h1>Jepsen Linearizability Report &nbsp;
  <span class="badge {{ 'pass' if result.linearizable else 'fail' }}">
    {{ '✓ LINEARIZABLE' if result.linearizable else '✗ NOT LINEARIZABLE' }}
  </span>
</h1>
<p style="color:var(--muted); margin-top:0.5rem; font-size:0.85rem;">{{ meta.timestamp }} &bull; {{ meta.test_duration_s | round(2) }}s &bull; model: {{ result.model }}</p>

<h2>Summary</h2>
<div class="grid">
  <div class="stat"><div class="stat-val">{{ meta.node_count }}</div><div class="stat-lbl">Nodes</div></div>
  <div class="stat"><div class="stat-val">{{ meta.client_count }}</div><div class="stat-lbl">Clients</div></div>
  <div class="stat"><div class="stat-val">{{ entries | length }}</div><div class="stat-lbl">Operations</div></div>
  <div class="stat"><div class="stat-val">{{ result.checked_ops }}</div><div class="stat-lbl">States checked</div></div>
  <div class="stat"><div class="stat-val">{{ result.elapsed_seconds | round(3) }}s</div><div class="stat-lbl">Check time</div></div>
  <div class="stat"><div class="stat-val">{{ faults | length }}</div><div class="stat-lbl">Fault events</div></div>
</div>

{% if faults %}
<h2>Injected Faults</h2>
<ul class="fault-list">
{% for f in faults %}
  <li><span>{{ f.type }}</span> at t={{ f.time | round(3) }}s — {{ f.detail }}</li>
{% endfor %}
</ul>
{% endif %}

<h2>Operation Timeline</h2>
<div class="timeline-wrap">
{{ svg }}
</div>

{% if not result.linearizable %}
<h2>Anomaly Analysis</h2>
<div style="background:var(--surface);border:1px solid #ef4444;border-radius:8px;padding:1rem;">
  <p style="color:#ef4444;font-weight:600;">History is NOT linearizable.</p>
  <p style="color:var(--muted);margin-top:0.5rem;font-size:0.85rem;">
    No valid sequential ordering of the {{ entries | length }} operations exists that satisfies
    the {{ result.model }} sequential specification while respecting real-time ordering.
    This indicates a consistency violation — likely caused by one of the injected faults
    (network partition, clock skew, or process crash) exposing a race condition in the pipeline.
  </p>
</div>
{% else %}
<h2>Linearization Order</h2>
<details open>
<summary>{{ result.linearization | length }} operations in valid sequential order</summary>
<pre>{% for e in result.linearization %}p{{ e.process }} {{ e.f }}({{ e.invoke_value }}) → {{ e.response_value }}
{% endfor %}</pre>
</details>
{% endif %}

<details>
<summary>Raw History ({{ ops | length }} events)</summary>
<pre>{{ raw_history }}</pre>
</details>

</body>
</html>
"""


@dataclass
class FaultEvent:
    type: str
    time: float
    detail: str


@dataclass
class ReportMeta:
    timestamp: str
    test_duration_s: float
    node_count: int
    client_count: int
    nemesis_description: str


def _build_svg(entries: List[Entry], width: int = 900) -> str:
    if not entries:
        return "<svg width='100' height='40'><text x='10' y='20'>No operations</text></svg>"

    processes = sorted(set(e.process for e in entries))
    lane_h = 36
    margin_left = 60
    margin_top = 30
    height = margin_top + len(processes) * lane_h + 20

    t_min = min(e.invoke_time for e in entries)
    t_max = max(e.response_time for e in entries)
    span = max(t_max - t_min, 0.001)
    usable_w = width - margin_left - 20

    def tx(t: float) -> float:
        return margin_left + (t - t_min) / span * usable_w

    lines = [f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">']

    # Grid lines every 0.5s
    step = 0.5
    t = t_min
    while t <= t_max + 0.01:
        x = tx(t)
        lbl = f"{t - t_min:.1f}s"
        lines.append(f'<line x1="{x:.1f}" y1="{margin_top}" x2="{x:.1f}" y2="{height - 10}" stroke="#334155" stroke-width="1"/>')
        lines.append(f'<text x="{x:.1f}" y="{margin_top - 6}" text-anchor="middle" fill="#64748b" font-size="10">{lbl}</text>')
        t += step

    # Process labels and lanes
    for i, pid in enumerate(processes):
        y = margin_top + i * lane_h + lane_h // 2
        lines.append(f'<text x="5" y="{y + 4}" fill="#94a3b8" font-size="11">p{pid}</text>')
        lines.append(f'<line x1="{margin_left}" y1="{y + 14}" x2="{width - 10}" y2="{y + 14}" stroke="#1e293b" stroke-width="1"/>')

    colors = {
        ("read",  True):  "#3b82f6",
        ("read",  False): "#f97316",
        ("write", True):  "#22c55e",
        ("write", False): "#ef4444",
        ("enqueue", True):  "#a855f7",
        ("enqueue", False): "#ef4444",
        ("dequeue", True):  "#06b6d4",
        ("dequeue", False): "#ef4444",
    }

    for entry in entries:
        i = processes.index(entry.process)
        y = margin_top + i * lane_h + 4
        bar_h = lane_h - 10
        x1 = tx(entry.invoke_time)
        x2 = tx(entry.response_time)
        bar_w = max(x2 - x1, 3)
        color = colors.get((entry.f, entry.ok), "#64748b")
        lbl = f"{entry.f[0].upper()}({entry.invoke_value})"
        if isinstance(entry.invoke_value, tuple) and len(entry.invoke_value) == 2:
            k, v = entry.invoke_value
            lbl = f"W {k}={v}"
        elif entry.f == "read":
            lbl = f"R {entry.invoke_value}→{entry.response_value}"

        lines.append(
            f'<rect x="{x1:.1f}" y="{y}" width="{bar_w:.1f}" height="{bar_h}" '
            f'rx="3" fill="{color}" opacity="0.85">'
            f'<title>{entry.f}({entry.invoke_value}) → {entry.response_value}</title>'
            f'</rect>'
        )
        if bar_w > 30:
            lines.append(
                f'<text x="{x1 + 3:.1f}" y="{y + bar_h - 4}" '
                f'font-size="9" fill="white" clip-path="url(#c{entry.index})">{lbl}</text>'
            )

    lines.append("</svg>")
    return "\n".join(lines)


def generate(
    ops: List[Op],
    entries: List[Entry],
    result: CheckResult,
    meta: ReportMeta,
    faults: List[FaultEvent],
    output_path: str,
) -> str:
    svg = _build_svg(entries)
    raw_history = json.dumps(
        [{"p": o.process, "type": o.type, "f": o.f, "value": o.value, "t": round(o.time, 6)} for o in ops],
        indent=2,
    )

    tmpl = Template(TEMPLATE)
    html = tmpl.render(
        result=result,
        entries=entries,
        ops=ops,
        meta=meta,
        faults=faults,
        svg=svg,
        raw_history=raw_history,
    )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)

    return output_path
