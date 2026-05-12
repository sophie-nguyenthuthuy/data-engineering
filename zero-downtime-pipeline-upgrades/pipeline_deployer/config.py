"""
Configuration dataclasses for the zero-downtime pipeline upgrade system.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DeploymentConfig:
    """
    Central configuration for a pipeline upgrade deployment.

    Attributes:
        divergence_threshold:       Max allowed divergence ratio (0.0–1.0) before
                                    promotion is blocked.  Default 1 %.
        rollback_threshold:         Divergence ratio that triggers an automatic
                                    rollback once v2 is carrying live traffic.
                                    Default 5 %.
        initial_v2_percentage:      Fraction of live traffic sent to v2 at the
                                    start of the shift phase (0.0 = pure shadow).
        traffic_shift_step:         How much to increase v2 traffic each step.
        traffic_shift_interval_sec: Seconds to wait between traffic-shift steps.
        min_samples_for_promotion:  Minimum shadow comparisons required before
                                    auto-promotion is allowed.
        comparison_window_size:     Rolling window of recent records used to
                                    compute the live divergence ratio.
        enable_auto_promotion:      Automatically promote v2 when divergence is
                                    below threshold and samples are sufficient.
        enable_auto_rollback:       Automatically roll back to v1 when divergence
                                    exceeds rollback_threshold.
        shadow_log_path:            Optional path to write per-record diff logs.
    """

    divergence_threshold: float = 0.01
    rollback_threshold: float = 0.05
    initial_v2_percentage: float = 0.0
    traffic_shift_step: float = 0.10
    traffic_shift_interval_sec: float = 60.0
    min_samples_for_promotion: int = 100
    comparison_window_size: int = 1000
    enable_auto_promotion: bool = True
    enable_auto_rollback: bool = True
    shadow_log_path: Optional[str] = None

    def validate(self) -> None:
        assert 0.0 <= self.divergence_threshold <= 1.0, "divergence_threshold must be in [0, 1]"
        assert 0.0 <= self.rollback_threshold <= 1.0, "rollback_threshold must be in [0, 1]"
        assert self.divergence_threshold <= self.rollback_threshold, (
            "rollback_threshold must be >= divergence_threshold"
        )
        assert 0.0 <= self.initial_v2_percentage <= 1.0, "initial_v2_percentage must be in [0, 1]"
        assert 0.0 < self.traffic_shift_step <= 1.0, "traffic_shift_step must be in (0, 1]"
        assert self.traffic_shift_interval_sec > 0, "traffic_shift_interval_sec must be positive"
        assert self.min_samples_for_promotion >= 1, "min_samples_for_promotion must be >= 1"
        assert self.comparison_window_size >= 1, "comparison_window_size must be >= 1"
