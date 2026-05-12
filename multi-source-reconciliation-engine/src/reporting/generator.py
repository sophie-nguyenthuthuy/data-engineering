"""Generate JSON and HTML reconciliation reports."""
from __future__ import annotations

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from ..classification.classifier import Discrepancy, DiscrepancyType
from ..matching.engine import MatchGroup

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Reconciliation Report — {run_id}</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --border: #2e3147;
    --text: #e2e8f0; --muted: #8892a4;
    --green: #22c55e; --yellow: #facc15; --orange: #f97316;
    --red: #ef4444; --blue: #60a5fa;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); padding: 2rem; }}
  h1 {{ font-size: 1.6rem; margin-bottom: .25rem; }}
  .meta {{ color: var(--muted); font-size: .85rem; margin-bottom: 2rem; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
  .kpi {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem; }}
  .kpi .val {{ font-size: 2rem; font-weight: 700; }}
  .kpi .lbl {{ color: var(--muted); font-size: .8rem; margin-top: .25rem; }}
  .kpi.green .val {{ color: var(--green); }}
  .kpi.yellow .val {{ color: var(--yellow); }}
  .kpi.orange .val {{ color: var(--orange); }}
  .kpi.red .val {{ color: var(--red); }}
  .kpi.blue .val {{ color: var(--blue); }}
  table {{ width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 10px; overflow: hidden; }}
  th {{ background: #232637; padding: .75rem 1rem; text-align: left; font-size: .8rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }}
  td {{ padding: .7rem 1rem; border-top: 1px solid var(--border); font-size: .875rem; }}
  tr:hover td {{ background: rgba(255,255,255,.03); }}
  .badge {{ display: inline-block; padding: .2rem .6rem; border-radius: 999px; font-size: .75rem; font-weight: 600; }}
  .NONE {{ background:#22c55e22; color:var(--green); }}
  .TIMING {{ background:#60a5fa22; color:var(--blue); }}
  .ROUNDING {{ background:#facc1522; color:var(--yellow); }}
  .AMOUNT_MISMATCH {{ background:#f9731622; color:var(--orange); }}
  .MISSING {{ background:#ef444422; color:var(--red); }}
  .MULTI {{ background:#a78bfa22; color:#a78bfa; }}
  .sev-LOW {{ background:#22c55e22; color:var(--green); }}
  .sev-MEDIUM {{ background:#facc1522; color:var(--yellow); }}
  .sev-HIGH {{ background:#f9731622; color:var(--orange); }}
  .sev-CRITICAL {{ background:#ef444422; color:var(--red); }}
  .conf {{ font-weight: 600; }}
  .conf-high {{ color: var(--green); }}
  .conf-med {{ color: var(--yellow); }}
  .conf-low {{ color: var(--orange); }}
  h2 {{ font-size: 1.1rem; margin: 2rem 0 1rem; }}
  .sla {{ color: var(--green); font-weight: 600; }}
  .sla.over {{ color: var(--red); }}
  details summary {{ cursor: pointer; font-size: .8rem; color: var(--muted); }}
  pre {{ font-size: .75rem; color: var(--muted); margin-top: .5rem; white-space: pre-wrap; }}
</style>
</head>
<body>
<h1>Multi-Source Reconciliation Report</h1>
<div class="meta">
  Run ID: <strong>{run_id}</strong> &nbsp;|&nbsp;
  Generated: <strong>{generated_at}</strong> &nbsp;|&nbsp;
  Elapsed: <span class="{sla_class}">{elapsed_s:.1f}s / {sla_s}s SLA</span>
</div>

<div class="kpi-grid">
  <div class="kpi blue"><div class="val">{total_groups}</div><div class="lbl">Total Match Groups</div></div>
  <div class="kpi green"><div class="val">{clean_pct}%</div><div class="lbl">Clean (no discrepancy)</div></div>
  <div class="kpi yellow"><div class="val">{disc_count}</div><div class="lbl">Discrepancies Found</div></div>
  <div class="kpi orange"><div class="val">{high_crit}</div><div class="lbl">High / Critical</div></div>
  <div class="kpi red"><div class="val">{missing_count}</div><div class="lbl">Missing Entries</div></div>
  <div class="kpi blue"><div class="val">{avg_conf}%</div><div class="lbl">Avg Match Confidence</div></div>
</div>

<h2>Discrepancy Breakdown</h2>
{breakdown_table}

<h2>All Match Groups</h2>
<table>
  <thead>
    <tr>
      <th>Group ID</th>
      <th>Sources Present</th>
      <th>Amount Range</th>
      <th>Date Range (days)</th>
      <th>Discrepancy Type</th>
      <th>Severity</th>
      <th>Confidence</th>
      <th>Details</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>
</body>
</html>"""


class ReportGenerator:
    def __init__(self, config: dict):
        self.output_dir = Path(config["reporting"]["output_dir"])
        self.formats = config["reporting"]["output_formats"]
        self.sla_s = config["reconciliation"]["sla_minutes"] * 60

    def generate(
        self,
        run_id: str,
        groups: list[MatchGroup],
        discrepancies: list[Discrepancy],
        elapsed_s: float,
    ) -> dict[str, Path]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        payload = self._build_payload(run_id, groups, discrepancies, elapsed_s)
        outputs: dict[str, Path] = {}

        if "json" in self.formats:
            p = self.output_dir / f"{run_id}.json"
            p.write_text(json.dumps(payload, indent=2, default=str))
            outputs["json"] = p

        if "html" in self.formats:
            p = self.output_dir / f"{run_id}.html"
            p.write_text(self._render_html(payload))
            outputs["html"] = p

        return outputs

    def _build_payload(
        self,
        run_id: str,
        groups: list[MatchGroup],
        discrepancies: list[Discrepancy],
        elapsed_s: float,
    ) -> dict[str, Any]:
        disc_map = {d.group_id: d for d in discrepancies}
        sla_met = elapsed_s <= self.sla_s

        breakdown: dict[str, int] = {}
        severity_counts: dict[str, int] = {}
        for d in discrepancies:
            key = d.primary_type.value
            breakdown[key] = breakdown.get(key, 0) + 1
            severity_counts[d.severity] = severity_counts.get(d.severity, 0) + 1

        group_records = []
        for g in groups:
            disc = disc_map.get(g.group_id)
            amounts = [t.amount for t in g.transactions.values()]
            dates = [t.value_date for t in g.transactions.values()]
            group_records.append({
                "group_id": g.group_id,
                "sources_present": sorted(g.transactions.keys()),
                "sources_missing": sorted(
                    set(["core_banking", "reporting_system", "third_party_aggregator", "manual_entries"])
                    - set(g.transactions.keys())
                ),
                "confidence": g.confidence,
                "match_scores": g.match_scores,
                "amount_min": min(amounts),
                "amount_max": max(amounts),
                "amount_range": round(max(amounts) - min(amounts), 4),
                "date_range_days": (max(dates) - min(dates)).days,
                "discrepancy_type": disc.primary_type.value if disc else "none",
                "discrepancy_types": [t.value for t in disc.types] if disc else [],
                "severity": disc.severity if disc else "NONE",
                "details": disc.details if disc else {},
                "transactions": {
                    s: {
                        "source_id": t.source_id,
                        "amount": t.amount,
                        "currency": t.currency,
                        "value_date": t.value_date.isoformat(),
                        "description": t.description,
                        "reference": t.reference,
                    }
                    for s, t in g.transactions.items()
                },
            })

        total = len(groups)
        clean = total - len(discrepancies)
        avg_conf = round(sum(g.confidence for g in groups) / total * 100, 1) if total else 0

        return {
            "run_id": run_id,
            "generated_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": round(elapsed_s, 2),
            "sla_seconds": self.sla_s,
            "sla_met": sla_met,
            "summary": {
                "total_groups": total,
                "clean_groups": clean,
                "clean_pct": round(clean / total * 100, 1) if total else 0,
                "discrepancy_count": len(discrepancies),
                "breakdown_by_type": breakdown,
                "breakdown_by_severity": severity_counts,
                "avg_confidence_pct": avg_conf,
            },
            "groups": group_records,
        }

    def _render_html(self, p: dict) -> str:
        s = p["summary"]
        high_crit = s["breakdown_by_severity"].get("HIGH", 0) + s["breakdown_by_severity"].get("CRITICAL", 0)
        sla_met = p["sla_met"]
        elapsed_s = p["elapsed_seconds"]

        rows_html = ""
        for g in p["groups"]:
            conf = g["confidence"]
            conf_cls = "conf-high" if conf >= 0.85 else ("conf-med" if conf >= 0.65 else "conf-low")
            dtype = g["discrepancy_type"].upper()
            det_json = json.dumps(g["details"], indent=2) if g["details"] else ""
            det_html = f"<details><summary>show</summary><pre>{det_json}</pre></details>" if det_json else "—"
            rows_html += (
                f"<tr>"
                f"<td>{g['group_id']}</td>"
                f"<td>{', '.join(g['sources_present'])}</td>"
                f"<td>{g['amount_range']:.4f}</td>"
                f"<td>{g['date_range_days']}</td>"
                f"<td><span class='badge {dtype}'>{dtype}</span></td>"
                f"<td><span class='badge sev-{g['severity']}'>{g['severity']}</span></td>"
                f"<td><span class='conf {conf_cls}'>{conf*100:.0f}%</span></td>"
                f"<td>{det_html}</td>"
                f"</tr>\n"
            )

        bd = s["breakdown_by_type"]
        bd_rows = "".join(
            f"<tr><td><span class='badge {k.upper()}'>{k}</span></td><td>{v}</td></tr>"
            for k, v in sorted(bd.items(), key=lambda x: -x[1])
        )
        bd_table = f"<table><thead><tr><th>Type</th><th>Count</th></tr></thead><tbody>{bd_rows}</tbody></table>"

        return _HTML_TEMPLATE.format(
            run_id=p["run_id"],
            generated_at=p["generated_at"],
            elapsed_s=elapsed_s,
            sla_s=self.sla_s,
            sla_class="sla" if sla_met else "sla over",
            total_groups=s["total_groups"],
            clean_pct=s["clean_pct"],
            disc_count=s["discrepancy_count"],
            high_crit=high_crit,
            missing_count=s["breakdown_by_type"].get("missing", 0),
            avg_conf=s["avg_confidence_pct"],
            breakdown_table=bd_table,
            rows=rows_html,
        )
