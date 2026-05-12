"""Online distribution fitting."""

from __future__ import annotations

from pwm.fit.evt import POTFitter
from pwm.fit.lognormal import LognormalFitter

__all__ = ["LognormalFitter", "POTFitter"]
