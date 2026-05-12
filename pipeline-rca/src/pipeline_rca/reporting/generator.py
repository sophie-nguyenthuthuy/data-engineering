"""Incident report generator — renders RootCauseReport to Markdown or HTML."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipeline_rca.models import RootCauseReport

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _avg(values: list[float]) -> float:
    valid = [v for v in values if not math.isnan(v)]
    return sum(valid) / len(valid) if valid else 0.0


def _build_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.globals["avg"] = _avg
    env.filters["avg"] = lambda seq: _avg([v for v in seq])
    env.filters["abs"] = abs
    return env


def _build_narrative(report: RootCauseReport) -> str:
    sig = [c for c in report.top_causes if c.is_significant]
    pct = abs(report.degradation.relative_change * 100)

    if not sig:
        return (
            f"Root cause analysis evaluated {len(report.all_candidates)} candidate change(s) "
            f"but found no statistically significant upstream cause for the "
            f"{pct:.1f}% {report.degradation.kind.value} in `{report.degradation.metric_name}`. "
            f"Manual investigation is recommended."
        )

    top = sig[0]
    change_desc = ""
    if top.change:
        change_desc = (
            f"a **{top.change.kind.value.replace('_', ' ')}** event on "
            f"`{top.change.table}`"
            + (f" (column `{top.change.column}`)" if top.change.column else "")
            + f" at {top.change.occurred_at.strftime('%Y-%m-%d %H:%M UTC')}"
        )
    else:
        change_desc = f"`{top.candidate}`"

    return (
        f"The ITS causal analysis attributes the {pct:.1f}% {report.degradation.kind.value} "
        f"with **{top.effect_size * 100:.1f}% relative effect** (p={top.p_value:.4f}) "
        f"to {change_desc}. "
        + (
            f"Additionally {len(sig) - 1} other significant candidate(s) were identified."
            if len(sig) > 1
            else ""
        )
    )


class ReportGenerator:
    """Render a RootCauseReport to Markdown (or optionally HTML)."""

    def __init__(self, output_dir: str | Path = "reports") -> None:
        self.output_dir = Path(output_dir)
        self._env = _build_env()

    def render_markdown(self, report: RootCauseReport) -> str:
        """Return the report as a Markdown string."""
        report.narrative = _build_narrative(report)
        tmpl = self._env.get_template("incident_report.md.j2")
        return tmpl.render(report=report)

    def save(self, report: RootCauseReport, fmt: str = "markdown") -> Path:
        """Render and write to disk; return the output path."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        filename = f"incident_{report.incident_id}_{ts}.md"
        out_path = self.output_dir / filename

        md = self.render_markdown(report)
        out_path.write_text(md, encoding="utf-8")
        return out_path
