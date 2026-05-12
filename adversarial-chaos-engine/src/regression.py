"""Emit pytest regression cases from discovered violations."""
from __future__ import annotations

import datetime as _dt
import textwrap

from .runner import Violation


def emit_pytest(violation: Violation) -> str:
    """Generate a pytest case stub from a Violation."""
    today = _dt.date.today().isoformat()
    body = textwrap.dedent(f"""\
        def test_{violation.fn_name}_violates_{violation.invariant.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '_')}():
            # Auto-discovered {today} by adversarial-chaos-engine
            from src import {violation.fn_name}
            df_in = {violation.input!r}
            df_out = {violation.fn_name}(df_in)
            # Expected: invariant {violation.invariant} should hold. It does not.
            # df_out was: {violation.output!r}
            assert False, "auto-generated; replace with proper assertion"
    """)
    return body


__all__ = ["emit_pytest"]
