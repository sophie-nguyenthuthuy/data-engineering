"""Report renderers: rich console, JSON, and HTML."""

from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import print as rprint

from ..models import AnalysisReport, Recommendation, ExpensivePattern, Severity

console = Console()

_SEVERITY_COLOR = {
    Severity.HIGH: "bold red",
    Severity.MEDIUM: "bold yellow",
    Severity.LOW: "dim cyan",
}
_SEVERITY_ICON = {Severity.HIGH: "🔴", Severity.MEDIUM: "🟡", Severity.LOW: "🔵"}


def _fmt_bytes(n: int) -> str:
    if n == 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    idx = min(int(math.log(n, 1024)), len(units) - 1)
    return f"{n / 1024**idx:.1f} {units[idx]}"


def _fmt_usd(v: float) -> str:
    return f"${v:,.2f}"


class ConsoleReporter:
    """Pretty-print the analysis report to the terminal using Rich."""

    def render(self, report: AnalysisReport) -> None:
        console.rule(
            f"[bold cyan]Cost & Performance Optimization Report "
            f"— {report.platform.value.upper()}[/bold cyan]"
        )
        console.print()

        # ── Summary panel ────────────────────────────────────────────────
        summary_lines = [
            f"[bold]Generated:[/bold]       {report.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
            f"[bold]Analysis window:[/bold] last {report.history_days} days",
            f"[bold]Queries analysed:[/bold] {report.total_queries_analyzed:,}",
            f"[bold]Total bytes scanned:[/bold] {_fmt_bytes(report.total_bytes_processed)}",
            f"[bold]Total cost:[/bold]      {_fmt_usd(report.total_cost_usd)}",
            f"[bold green]Potential savings:[/bold green]  "
            f"[bold green]{_fmt_usd(report.total_estimated_savings_usd)}/month[/bold green]",
        ]
        console.print(Panel("\n".join(summary_lines), title="Summary", border_style="cyan"))
        console.print()

        # ── Top expensive tables ─────────────────────────────────────────
        if report.top_tables:
            tbl = Table(
                title="Top Tables by Cost",
                box=box.ROUNDED,
                show_lines=False,
                header_style="bold magenta",
            )
            tbl.add_column("Table", style="cyan", no_wrap=True, max_width=55)
            tbl.add_column("Queries", justify="right")
            tbl.add_column("Bytes Scanned", justify="right")
            tbl.add_column("Total Cost", justify="right")
            tbl.add_column("Size", justify="right")
            for ts in report.top_tables[:10]:
                tbl.add_row(
                    ts.table_id.split(".")[-1],
                    str(ts.query_count),
                    _fmt_bytes(ts.total_bytes_scanned),
                    _fmt_usd(ts.total_cost_usd),
                    _fmt_bytes(ts.size_bytes) if ts.size_bytes else "—",
                )
            console.print(tbl)
            console.print()

        # ── Recommendations ──────────────────────────────────────────────
        if report.recommendations:
            console.print("[bold underline]Recommendations[/bold underline]")
            for i, rec in enumerate(report.recommendations, 1):
                sev_color = _SEVERITY_COLOR[rec.severity]
                sev_icon = _SEVERITY_ICON[rec.severity]
                console.print(
                    f"\n[{sev_color}]{sev_icon} [{rec.severity.value.upper()}][/{sev_color}]  "
                    f"[bold]{rec.title}[/bold]   "
                    f"[green]~{_fmt_usd(rec.estimated_savings_usd_monthly)}/mo savings[/green]"
                )
                console.print(f"   {rec.description}")
                console.print(f"   [dim]Affects {rec.affected_query_count} queries[/dim]")
                console.print()
                console.print(
                    Panel(
                        rec.action,
                        title="Suggested Action",
                        border_style="dim",
                        padding=(0, 1),
                    )
                )
        else:
            console.print("[green]No structural recommendations — tables look well-optimised.[/green]")

        console.print()

        # ── Expensive patterns ───────────────────────────────────────────
        if report.expensive_patterns:
            console.print("[bold underline]Expensive Query Patterns[/bold underline]")
            for pat in report.expensive_patterns:
                sev_color = _SEVERITY_COLOR[pat.severity]
                sev_icon = _SEVERITY_ICON[pat.severity]
                console.print(
                    f"\n[{sev_color}]{sev_icon} {pat.pattern_name}[/{sev_color}]   "
                    f"[dim]{pat.query_count} queries · "
                    f"{_fmt_usd(pat.total_cost_usd)} total · "
                    f"~{_fmt_usd(pat.estimated_savings_usd)} saveable[/dim]"
                )
                console.print(f"   {pat.description}")
                console.print(f"   [bold]Fix:[/bold] {pat.fix_suggestion}")
                if pat.example_queries:
                    console.print(
                        Panel(
                            pat.example_queries[0][:300] + ("…" if len(pat.example_queries[0]) > 300 else ""),
                            title="Example query",
                            border_style="dim",
                            padding=(0, 1),
                        )
                    )
        else:
            console.print("[green]No common expensive patterns detected.[/green]")

        console.print()
        console.rule("[dim]End of Report[/dim]")


class JsonReporter:
    """Write the report to a JSON file."""

    def render(self, report: AnalysisReport, output_path: str | Path) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        def _default(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            if hasattr(obj, "value"):
                return obj.value
            raise TypeError(f"Not serialisable: {type(obj)}")

        data = {
            "platform": report.platform.value,
            "generated_at": report.generated_at.isoformat(),
            "history_days": report.history_days,
            "total_queries_analyzed": report.total_queries_analyzed,
            "total_cost_usd": report.total_cost_usd,
            "total_bytes_processed": report.total_bytes_processed,
            "total_estimated_savings_usd_monthly": report.total_estimated_savings_usd,
            "recommendations": [
                {
                    "type": r.rec_type.value,
                    "severity": r.severity.value,
                    "table": r.table_id,
                    "title": r.title,
                    "description": r.description,
                    "action": r.action,
                    "estimated_savings_usd_monthly": r.estimated_savings_usd_monthly,
                    "affected_query_count": r.affected_query_count,
                    "evidence": r.evidence,
                }
                for r in report.recommendations
            ],
            "expensive_patterns": [
                {
                    "pattern": p.pattern_name,
                    "severity": p.severity.value,
                    "description": p.description,
                    "query_count": p.query_count,
                    "total_cost_usd": p.total_cost_usd,
                    "estimated_savings_usd": p.estimated_savings_usd,
                    "fix_suggestion": p.fix_suggestion,
                    "example_queries": p.example_queries,
                }
                for p in report.expensive_patterns
            ],
            "top_tables": [
                {
                    "table_id": t.table_id,
                    "query_count": t.query_count,
                    "total_bytes_scanned": t.total_bytes_scanned,
                    "total_cost_usd": t.total_cost_usd,
                    "size_bytes": t.size_bytes,
                }
                for t in report.top_tables[:20]
            ],
        }
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, default=_default)

        console.print(f"[green]JSON report written to {output_path}[/green]")


class HtmlReporter:
    """Write a self-contained HTML report."""

    def render(self, report: AnalysisReport, output_path: str | Path) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        sev_badge = {
            "high": '<span style="background:#dc2626;color:#fff;padding:2px 8px;border-radius:4px;font-size:.8em">HIGH</span>',
            "medium": '<span style="background:#d97706;color:#fff;padding:2px 8px;border-radius:4px;font-size:.8em">MEDIUM</span>',
            "low": '<span style="background:#2563eb;color:#fff;padding:2px 8px;border-radius:4px;font-size:.8em">LOW</span>',
        }

        def rec_rows() -> str:
            rows = []
            for r in report.recommendations:
                rows.append(f"""
                <tr>
                  <td>{sev_badge.get(r.severity.value,'')}</td>
                  <td><strong>{r.title}</strong><br><small>{r.description}</small></td>
                  <td style="font-family:monospace;font-size:.8em;white-space:pre-wrap">{r.action}</td>
                  <td style="text-align:right;color:#16a34a"><strong>${r.estimated_savings_usd_monthly:,.2f}</strong></td>
                  <td style="text-align:right">{r.affected_query_count:,}</td>
                </tr>""")
            return "\n".join(rows)

        def pat_rows() -> str:
            rows = []
            for p in report.expensive_patterns:
                rows.append(f"""
                <tr>
                  <td>{sev_badge.get(p.severity.value,'')}</td>
                  <td><strong>{p.pattern_name}</strong><br><small>{p.description}</small><br>
                      <em>Fix: {p.fix_suggestion}</em></td>
                  <td style="text-align:right">{p.query_count:,}</td>
                  <td style="text-align:right">${p.total_cost_usd:,.2f}</td>
                  <td style="text-align:right;color:#16a34a"><strong>${p.estimated_savings_usd:,.2f}</strong></td>
                </tr>""")
            return "\n".join(rows)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Query Cost Optimizer — {report.platform.value.upper()} Report</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 0; padding: 24px; background: #f8fafc; color: #1e293b; }}
  h1 {{ color: #0f172a; }}
  .summary {{ display:flex; gap:16px; flex-wrap:wrap; margin-bottom:24px; }}
  .card {{ background:#fff; border:1px solid #e2e8f0; border-radius:8px; padding:16px 24px; min-width:160px; }}
  .card .label {{ font-size:.75em; color:#64748b; text-transform:uppercase; letter-spacing:.05em; }}
  .card .value {{ font-size:1.5em; font-weight:700; margin-top:4px; }}
  .savings {{ color:#16a34a; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:8px; overflow:hidden;
           box-shadow:0 1px 3px rgba(0,0,0,.07); margin-bottom:32px; }}
  th {{ background:#1e293b; color:#fff; padding:10px 14px; text-align:left; font-size:.85em; }}
  td {{ padding:10px 14px; border-bottom:1px solid #f1f5f9; font-size:.9em; vertical-align:top; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:#f8fafc; }}
  pre {{ margin:0; }}
</style>
</head>
<body>
<h1>⚡ Query Cost & Performance Report</h1>
<p><strong>Platform:</strong> {report.platform.value.upper()} &nbsp;|&nbsp;
   <strong>Generated:</strong> {report.generated_at.strftime('%Y-%m-%d %H:%M UTC')} &nbsp;|&nbsp;
   <strong>Window:</strong> last {report.history_days} days</p>

<div class="summary">
  <div class="card"><div class="label">Queries Analysed</div><div class="value">{report.total_queries_analyzed:,}</div></div>
  <div class="card"><div class="label">Total Bytes Scanned</div><div class="value">{_fmt_bytes(report.total_bytes_processed)}</div></div>
  <div class="card"><div class="label">Total Cost</div><div class="value">${report.total_cost_usd:,.2f}</div></div>
  <div class="card"><div class="label">Potential Monthly Savings</div><div class="value savings">${report.total_estimated_savings_usd:,.2f}</div></div>
</div>

<h2>Recommendations</h2>
<table>
  <thead><tr><th>Severity</th><th>Recommendation</th><th>Suggested Action</th><th>Est. Savings/mo</th><th>Queries</th></tr></thead>
  <tbody>{rec_rows() or '<tr><td colspan="5" style="color:#64748b">No recommendations — tables look well-optimised.</td></tr>'}</tbody>
</table>

<h2>Expensive Patterns</h2>
<table>
  <thead><tr><th>Severity</th><th>Pattern</th><th>Query Count</th><th>Total Cost</th><th>Saveable</th></tr></thead>
  <tbody>{pat_rows() or '<tr><td colspan="5" style="color:#64748b">No expensive patterns detected.</td></tr>'}</tbody>
</table>

<h2>Top Tables by Cost</h2>
<table>
  <thead><tr><th>Table</th><th>Queries</th><th>Bytes Scanned</th><th>Total Cost</th><th>Table Size</th></tr></thead>
  <tbody>
  {"".join(f'<tr><td>{t.table_id}</td><td style="text-align:right">{t.query_count:,}</td><td style="text-align:right">{_fmt_bytes(t.total_bytes_scanned)}</td><td style="text-align:right">${t.total_cost_usd:,.2f}</td><td style="text-align:right">{_fmt_bytes(t.size_bytes) if t.size_bytes else "—"}</td></tr>' for t in report.top_tables[:15])}
  </tbody>
</table>
<p style="color:#94a3b8;font-size:.8em">Generated by <strong>query-cost-optimizer</strong></p>
</body>
</html>"""

        with open(output_path, "w") as f:
            f.write(html)
        console.print(f"[green]HTML report written to {output_path}[/green]")
