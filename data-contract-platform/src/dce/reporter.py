"""Generate structured breaking-change and validation reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contract import DataContract, load_contracts_dir
from .scorer import ProducerScore
from .validator import ValidationResult


# ------------------------------------------------------------------ #
# Breaking-change report
# ------------------------------------------------------------------ #

def breaking_change_report(
    old: DataContract,
    new: DataContract,
) -> dict[str, Any]:
    """Produce a structured diff report between two contract versions."""
    breaking = new.is_breaking_change_from(old) if old.semver < new.semver else old.is_breaking_change_from(new)

    added_fields = [
        f.name for f in new.fields
        if f.name not in {x.name for x in old.fields}
    ]
    removed_fields = [
        f.name for f in old.fields
        if f.name not in {x.name for x in new.fields}
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract_id": new.id,
        "from_version": old.version,
        "to_version": new.version,
        "producer": new.producer,
        "consumers": new.consumers,
        "is_breaking": len(breaking) > 0,
        "breaking_changes": breaking,
        "added_fields": added_fields,
        "removed_fields": removed_fields,
        "summary": (
            f"{len(breaking)} breaking change(s), "
            f"{len(added_fields)} addition(s), "
            f"{len(removed_fields)} removal(s)"
        ),
    }


def contracts_dir_breaking_changes(contracts_dir: Path | str) -> list[dict[str, Any]]:
    """Scan a contracts directory and report all consecutive-version breaking changes."""
    registry = load_contracts_dir(contracts_dir)
    reports: list[dict[str, Any]] = []
    for _cid, versions in registry.items():
        for old, new in zip(versions, versions[1:]):
            report = breaking_change_report(old, new)
            if report["is_breaking"]:
                reports.append(report)
    return reports


# ------------------------------------------------------------------ #
# Validation summary report
# ------------------------------------------------------------------ #

def validation_summary_report(
    results: list[ValidationResult],
    scores: list[ProducerScore],
) -> dict[str, Any]:
    total = len(results)
    passed = sum(1 for r in results if r.passed)

    producer_map: dict[str, list[ValidationResult]] = {}
    for r in results:
        producer_map.setdefault(r.producer, []).append(r)

    score_map = {(s.producer, s.contract_id): s for s in scores}

    producers_detail = []
    for producer, runs in producer_map.items():
        for run in runs:
            key = (run.producer, run.contract_id)
            s = score_map.get(key)
            producers_detail.append({
                "producer": producer,
                "contract_id": run.contract_id,
                "latest_run_passed": run.passed,
                "latest_error_count": len(run.errors()),
                "latest_warning_count": len(run.warnings()),
                "reliability_score": s.reliability_score if s else None,
                "total_runs": s.total_runs if s else 1,
            })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_validations": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round(passed / total, 4) if total else 0,
        "producers": producers_detail,
    }


# ------------------------------------------------------------------ #
# Consumer notification payload
# ------------------------------------------------------------------ #

def consumer_notification(
    result: ValidationResult,
    breaking_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the structured payload sent to downstream consumers."""
    payload: dict[str, Any] = {
        "notification_type": "validation_failure" if not result.passed else "validation_ok",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract_id": result.contract_id,
        "contract_version": result.contract_version,
        "producer": result.producer,
        "validated_at": result.validated_at,
        "passed": result.passed,
        "error_count": len(result.errors()),
        "warning_count": len(result.warnings()),
        "issues": [
            {
                "rule": i.rule,
                "severity": i.severity,
                "message": i.message,
            }
            for i in result.issues
        ],
    }
    if breaking_report:
        payload["breaking_change_report"] = breaking_report
    return payload


# ------------------------------------------------------------------ #
# File output helpers
# ------------------------------------------------------------------ #

def write_report(report: dict, output_path: Path | str, *, indent: int = 2) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as fh:
        json.dump(report, fh, indent=indent, default=str)


def write_markdown_report(report: dict, output_path: Path | str) -> None:
    """Render a breaking-change report as Markdown."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"# Breaking Change Report: `{report['contract_id']}`",
        "",
        f"**From:** `{report['from_version']}` → **To:** `{report['to_version']}`  ",
        f"**Producer:** {report['producer']}  ",
        f"**Generated:** {report['generated_at']}  ",
        f"**Summary:** {report['summary']}",
        "",
    ]

    if report["breaking_changes"]:
        lines += ["## Breaking Changes", ""]
        for change in report["breaking_changes"]:
            lines.append(f"- ⚠️ {change}")
        lines.append("")

    if report["added_fields"]:
        lines += ["## Added Fields", ""]
        for f in report["added_fields"]:
            lines.append(f"- ✅ `{f}`")
        lines.append("")

    if report["removed_fields"]:
        lines += ["## Removed Fields", ""]
        for f in report["removed_fields"]:
            lines.append(f"- ❌ `{f}`")
        lines.append("")

    if report["consumers"]:
        lines += ["## Affected Consumers", ""]
        for c in report["consumers"]:
            lines.append(f"- {c}")
        lines.append("")

    output_path.write_text("\n".join(lines))
