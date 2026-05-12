"""Eval harness. Runs the classifier over golden.json, computes per-label
precision/recall/F1, writes results + a confusion matrix to the warehouse
so the dashboard can chart drift over time.
"""
from __future__ import annotations

import json
import uuid
from collections import defaultdict
from pathlib import Path

from ..stubs import llm, warehouse

GOLDEN = Path(__file__).parent / "golden.json"


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f1


def run() -> dict:
    golden = json.loads(GOLDEN.read_text())
    labels = golden["labels"]
    run_id = str(uuid.uuid4())
    warehouse.start_run(run_id, kind="eval", tenant_id=golden["tenant"])

    tp = defaultdict(int)
    fp = defaultdict(int)
    fn = defaultdict(int)
    support = defaultdict(int)
    confusion: list[dict] = []

    for case in golden["cases"]:
        try:
            cls = llm.classify(case["subject"], case["body"], labels)
            pred = cls.predicted_label
        except Exception:
            pred = "spam"  # treat unclassifiable as spam for eval
        expected = case["expected"]
        support[expected] += 1
        confusion.append({"expected": expected, "predicted": pred, "subject": case["subject"]})
        if pred == expected:
            tp[expected] += 1
        else:
            fp[pred] += 1
            fn[expected] += 1

    per_label = []
    macro_f1 = 0.0
    for lbl in labels:
        p, r, f1 = _prf(tp[lbl], fp[lbl], fn[lbl])
        per_label.append({
            "label": lbl, "precision": p, "recall": r, "f1": f1, "support": support[lbl],
        })
        macro_f1 += f1
    macro_f1 /= len(labels)

    warehouse.write_eval(run_id, per_label)
    warehouse.finish_run(
        run_id, "ok",
        details=json.dumps({"macro_f1": round(macro_f1, 3), "confusion": confusion}),
    )
    return {"run_id": run_id, "macro_f1": macro_f1, "per_label": per_label}
