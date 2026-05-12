"""
JIT-compile Pattern objects into Numba nopython functions.

For each Pattern we generate a Python source string that describes a tight
state-machine loop operating on flat numpy arrays, then write it to a file
in a per-user cache directory and import it — this lets Numba cache the
compiled bytecode to disk so subsequent process starts skip recompilation.

State arrays (per compiled pattern, indexed by entity_id):
  step_arr      int8    — current step index (0 = idle)
  count_arr     int32   — events seen at the current step
  start_ts_arr  int64   — anchor timestamp (first step hit)
  last_ts_arr   int64   — timestamp of most recent step hit
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from .pattern import Pattern, StepPredicate

try:
    import numba  # noqa: F401
    from numba import njit as _njit  # noqa: F401

    _NUMBA_AVAILABLE = True
except ImportError:
    _NUMBA_AVAILABLE = False

_CEP_CACHE_DIR = Path(os.environ.get("CEP_CACHE_DIR", Path.home() / ".cache" / "cep_engine"))


MAX_ENTITIES = 1 << 20  # 1 M entity slots (entity_id % MAX_ENTITIES)


@dataclass
class CompiledPattern:
    name: str
    match_fn: Callable  # (type_id, entity_id, ts, value, flags, step, count, start_ts, last_ts) -> bool
    step_arr: np.ndarray       # int8[MAX_ENTITIES]
    count_arr: np.ndarray      # int32[MAX_ENTITIES]
    start_ts_arr: np.ndarray   # int64[MAX_ENTITIES]
    last_ts_arr: np.ndarray    # int64[MAX_ENTITIES]

    def reset_entity(self, entity_id: int) -> None:
        slot = entity_id % MAX_ENTITIES
        self.step_arr[slot] = 0
        self.count_arr[slot] = 0
        self.start_ts_arr[slot] = 0
        self.last_ts_arr[slot] = 0


# ---------------------------------------------------------------------------
# Code generation helpers

def _pred_conditions(pred: StepPredicate, indent: int) -> list[str]:
    """Return a list of boolean sub-expressions for *pred*."""
    pad = " " * indent
    lines: list[str] = []
    lines.append(f"{pad}event_type == np.int32({pred.type_id})")
    if pred.value_gte is not None:
        lines.append(f"{pad}and value >= np.float64({pred.value_gte})")
    if pred.value_lte is not None:
        lines.append(f"{pad}and value <= np.float64({pred.value_lte})")
    if pred.flags_mask:
        lines.append(f"{pad}and (flags & np.uint32({pred.flags_mask})) == np.uint32({pred.flags_value})")
    return lines


def _generate_source(pattern: Pattern) -> str:
    steps = pattern.steps
    window = pattern._total_window_ns
    lines: list[str] = []

    lines += [
        "import numpy as np",
        "from numba import njit",
        "",
        "@njit(cache=True)",
        f"def _cep_match_{pattern.name}(",
        "    event_type, entity_id, timestamp, value, flags,",
        "    step_arr, count_arr, start_ts_arr, last_ts_arr,",
        "):",
        f"    slot = entity_id % np.int64({MAX_ENTITIES})",
        "    step      = np.int8(step_arr[slot])",
        "    cnt       = np.int32(count_arr[slot])",
        "    start_ts  = np.int64(start_ts_arr[slot])",
        "    last_ts   = np.int64(last_ts_arr[slot])",
        "    matched   = False",
        "",
    ]

    for i, pred in enumerate(steps):
        is_last = i == len(steps) - 1
        conds = _pred_conditions(pred, indent=8)
        cond_str = (" \\\n" + " " * 12).join(conds)

        if i == 0:
            # Anchor step — always reachable from idle state
            lines += [
                f"    if step == np.int8(0):",
                f"        if ({cond_str}):",
                f"            cnt = np.int32(cnt + np.int32(1))",
                f"            if cnt >= np.int32({pred.count}):",
                f"                step_arr[slot] = np.int8({i + 1})",
                f"                count_arr[slot] = np.int32(0)",
                f"                start_ts_arr[slot] = timestamp",
                f"                last_ts_arr[slot]  = timestamp",
                f"            else:",
                f"                count_arr[slot] = cnt",
            ]
        else:
            gap_check = ""
            if pred.max_gap_ns is not None:
                gap_check = (
                    f" and timestamp - last_ts <= np.int64({pred.max_gap_ns})"
                )
            lines += [
                f"    elif step == np.int8({i}):",
                f"        # Check total window expiry",
                f"        if timestamp - start_ts > np.int64({window}):",
                f"            step_arr[slot]     = np.int8(0)",
                f"            count_arr[slot]    = np.int32(0)",
                f"            start_ts_arr[slot] = np.int64(0)",
                f"            last_ts_arr[slot]  = np.int64(0)",
                f"        elif ({cond_str}){gap_check}:",
                f"            cnt = np.int32(cnt + np.int32(1))",
                f"            if cnt >= np.int32({pred.count}):",
            ]
            if is_last:
                lines += [
                    f"                matched = True",
                    f"                step_arr[slot]     = np.int8(0)",
                    f"                count_arr[slot]    = np.int32(0)",
                    f"                start_ts_arr[slot] = np.int64(0)",
                    f"                last_ts_arr[slot]  = np.int64(0)",
                ]
            else:
                lines += [
                    f"                step_arr[slot]    = np.int8({i + 1})",
                    f"                count_arr[slot]   = np.int32(0)",
                    f"                last_ts_arr[slot] = timestamp",
                ]
            lines += [
                f"            else:",
                f"                count_arr[slot]   = cnt",
                f"                last_ts_arr[slot] = timestamp",
            ]

    lines += [
        "",
        "    return matched",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------

class PatternCompiler:
    """Compile Pattern objects into Numba-JIT'd CompiledPattern instances."""

    def compile(self, pattern: Pattern, max_entities: int = MAX_ENTITIES) -> CompiledPattern:
        if not pattern.steps:
            raise ValueError(f"Pattern {pattern.name!r} has no steps")

        source = _generate_source(pattern)
        raw_fn = _load_or_compile(pattern.name, source)

        # Trigger Numba JIT immediately so the first real call is fast
        _warmup(raw_fn, max_entities)

        return CompiledPattern(
            name=pattern.name,
            match_fn=raw_fn,
            step_arr=np.zeros(max_entities, dtype=np.int8),
            count_arr=np.zeros(max_entities, dtype=np.int32),
            start_ts_arr=np.zeros(max_entities, dtype=np.int64),
            last_ts_arr=np.zeros(max_entities, dtype=np.int64),
        )

    def source(self, pattern: Pattern) -> str:
        """Return the generated source for inspection / debugging."""
        return _generate_source(pattern)


def _load_or_compile(name: str, source: str) -> Callable:
    """
    Write *source* to a stable cache path and import it so Numba can cache
    the compiled bytecode to disk (``cache=True`` requires a real file).
    """
    _CEP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(source.encode()).hexdigest()[:12]
    mod_name = f"_cep_gen_{name}_{digest}"
    cache_path = _CEP_CACHE_DIR / f"{mod_name}.py"

    if not cache_path.exists():
        cache_path.write_text(source)

    if mod_name in sys.modules:
        mod = sys.modules[mod_name]
    else:
        spec = importlib.util.spec_from_file_location(mod_name, cache_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)

    return getattr(mod, f"_cep_match_{name}")


def _warmup(fn: Callable, max_entities: int = MAX_ENTITIES) -> None:
    """Call with dummy data to force Numba to compile the specialisation."""
    fn(
        np.int32(0), np.int64(0), np.int64(0), np.float64(0.0), np.uint32(0),
        np.zeros(max_entities, np.int8),
        np.zeros(max_entities, np.int32),
        np.zeros(max_entities, np.int64),
        np.zeros(max_entities, np.int64),
    )


# ---------------------------------------------------------------------------
# Pure-Python fallback (used when Numba is not installed)

def _make_python_fallback(pattern: Pattern) -> Callable:
    steps = pattern.steps
    window = pattern._total_window_ns

    def match_fn(event_type, entity_id, timestamp, value, flags,
                 step_arr, count_arr, start_ts_arr, last_ts_arr):
        slot = entity_id % MAX_ENTITIES
        step = int(step_arr[slot])
        cnt = int(count_arr[slot])
        start_ts = int(start_ts_arr[slot])
        last_ts = int(last_ts_arr[slot])
        matched = False

        def reset():
            step_arr[slot] = 0
            count_arr[slot] = 0
            start_ts_arr[slot] = 0
            last_ts_arr[slot] = 0

        def _check(pred: StepPredicate) -> bool:
            if event_type != pred.type_id:
                return False
            if pred.value_gte is not None and value < pred.value_gte:
                return False
            if pred.value_lte is not None and value > pred.value_lte:
                return False
            if pred.flags_mask and (flags & pred.flags_mask) != pred.flags_value:
                return False
            return True

        if step == 0:
            pred = steps[0]
            if _check(pred):
                cnt += 1
                if cnt >= pred.count:
                    step_arr[slot] = 1
                    count_arr[slot] = 0
                    start_ts_arr[slot] = timestamp
                    last_ts_arr[slot] = timestamp
                else:
                    count_arr[slot] = cnt
        else:
            if timestamp - start_ts > window:
                reset()
            else:
                pred = steps[step]
                gap_ok = pred.max_gap_ns is None or (timestamp - last_ts <= pred.max_gap_ns)
                if _check(pred) and gap_ok:
                    cnt += 1
                    if cnt >= pred.count:
                        if step == len(steps) - 1:
                            matched = True
                            reset()
                        else:
                            step_arr[slot] = step + 1
                            count_arr[slot] = 0
                            last_ts_arr[slot] = timestamp
                    else:
                        count_arr[slot] = cnt
                        last_ts_arr[slot] = timestamp

        return matched

    return match_fn
