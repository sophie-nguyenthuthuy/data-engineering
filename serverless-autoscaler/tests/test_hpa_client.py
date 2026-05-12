from __future__ import annotations

import pytest

from autoscaler.config import HPAConfig
from autoscaler.hpa_client import HPAClient


@pytest.fixture
def client():
    cfg = HPAConfig(
        namespace="default",
        scale_up_cooldown_seconds=0,
        scale_down_cooldown_seconds=0,
    )
    return HPAClient(cfg)


class TestHPAClient:
    def test_get_hpa_returns_stub_data(self, client):
        hpa = client.get_hpa("my-hpa")
        assert hpa["name"] == "my-hpa"
        assert hpa["min_replicas"] >= 1

    def test_prewarm_returns_action(self, client):
        action = client.prewarm("j1", "my-hpa", target_min=5, target_max=20)
        assert action is not None
        assert action.min_replicas_after == 5
        assert action.max_replicas_after == 20

    def test_cooldown_blocks_second_patch(self, client):
        # Set a large cooldown
        client._cfg.scale_up_cooldown_seconds = 9999
        client.prewarm("j1", "my-hpa", target_min=5, target_max=20)
        action2 = client.prewarm("j1", "my-hpa", target_min=8, target_max=25)
        assert action2 is None

    def test_min_replicas_floor_respected(self, client):
        client._cfg.min_replicas_floor = 3
        action = client.prewarm("j1", "my-hpa", target_min=1, target_max=10)
        assert action.min_replicas_after == 3

    def test_max_replicas_ceiling_respected(self, client):
        client._cfg.max_replicas_ceiling = 50
        action = client.prewarm("j1", "my-hpa", target_min=5, target_max=9999)
        assert action.max_replicas_after == 50

    def test_restore_defaults(self, client):
        action = client.restore_defaults("j1", "my-hpa", default_min=1, default_max=10)
        assert action is not None
        assert action.reason == "post_job_restore_defaults"
