from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from .config import HPAConfig
from .models import ScalingAction

logger = logging.getLogger(__name__)


class HPAClient:
    """
    Thin wrapper around the Kubernetes autoscaling/v2 HPA API.

    Reads current HPA state and patches minReplicas / maxReplicas
    to implement predictive pre-warming and mid-run adjustments.
    """

    def __init__(self, cfg: HPAConfig) -> None:
        self._cfg = cfg
        self._client = self._build_client()
        self._last_scale_up: dict[str, datetime] = {}
        self._last_scale_down: dict[str, datetime] = {}

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def get_hpa(self, name: str, namespace: Optional[str] = None) -> dict:
        ns = namespace or self._cfg.namespace
        autoscaling = self._client.AutoscalingV2Api()
        hpa = autoscaling.read_namespaced_horizontal_pod_autoscaler(name, ns)
        return {
            "name": hpa.metadata.name,
            "namespace": hpa.metadata.namespace,
            "min_replicas": hpa.spec.min_replicas or 1,
            "max_replicas": hpa.spec.max_replicas,
            "current_replicas": hpa.status.current_replicas or 0,
            "desired_replicas": hpa.status.desired_replicas or 0,
        }

    def prewarm(
        self,
        job_id: str,
        hpa_name: str,
        target_min: int,
        target_max: int,
        namespace: Optional[str] = None,
    ) -> Optional[ScalingAction]:
        """Raise minReplicas before a job starts to avoid cold starts."""
        return self._patch(
            job_id=job_id,
            hpa_name=hpa_name,
            namespace=namespace,
            new_min=target_min,
            new_max=target_max,
            reason=f"predictive_prewarm min={target_min} max={target_max}",
            is_scale_up=True,
        )

    def adjust_mid_run(
        self,
        job_id: str,
        hpa_name: str,
        new_min: int,
        new_max: int,
        namespace: Optional[str] = None,
        reason: str = "mid_run_adjustment",
    ) -> Optional[ScalingAction]:
        """Adjust HPA bounds while a job is running based on live metrics."""
        return self._patch(
            job_id=job_id,
            hpa_name=hpa_name,
            namespace=namespace,
            new_min=new_min,
            new_max=new_max,
            reason=reason,
            is_scale_up=new_min > self._current_min(hpa_name, namespace),
        )

    def restore_defaults(
        self,
        job_id: str,
        hpa_name: str,
        default_min: int = 1,
        default_max: int = 10,
        namespace: Optional[str] = None,
    ) -> Optional[ScalingAction]:
        """Return HPA to baseline after job completion."""
        return self._patch(
            job_id=job_id,
            hpa_name=hpa_name,
            namespace=namespace,
            new_min=default_min,
            new_max=default_max,
            reason="post_job_restore_defaults",
            is_scale_up=False,
        )

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _patch(
        self,
        job_id: str,
        hpa_name: str,
        namespace: Optional[str],
        new_min: int,
        new_max: int,
        reason: str,
        is_scale_up: bool,
    ) -> Optional[ScalingAction]:
        ns = namespace or self._cfg.namespace
        now = datetime.utcnow()

        # Cooldown check
        cooldown = (
            self._cfg.scale_up_cooldown_seconds
            if is_scale_up
            else self._cfg.scale_down_cooldown_seconds
        )
        tracker = self._last_scale_up if is_scale_up else self._last_scale_down
        last = tracker.get(hpa_name)
        if last and (now - last).total_seconds() < cooldown:
            logger.debug(
                "hpa=%s cooldown active (%.0fs remaining), skipping patch",
                hpa_name,
                cooldown - (now - last).total_seconds(),
            )
            return None

        new_min = max(self._cfg.min_replicas_floor, new_min)
        new_max = min(self._cfg.max_replicas_ceiling, new_max)

        try:
            current = self.get_hpa(hpa_name, ns)
            autoscaling = self._client.AutoscalingV2Api()
            patch_body = {
                "spec": {
                    "minReplicas": new_min,
                    "maxReplicas": new_max,
                }
            }
            autoscaling.patch_namespaced_horizontal_pod_autoscaler(
                hpa_name, ns, patch_body
            )
            tracker[hpa_name] = now
            action = ScalingAction(
                job_id=job_id,
                hpa_target=hpa_name,
                namespace=ns,
                action_at=now,
                min_replicas_before=current["min_replicas"],
                min_replicas_after=new_min,
                max_replicas_before=current["max_replicas"],
                max_replicas_after=new_max,
                reason=reason,
            )
            logger.info(
                "Patched HPA job=%s hpa=%s min %d→%d max %d→%d reason=%s",
                job_id, hpa_name,
                action.min_replicas_before, new_min,
                action.max_replicas_before, new_max,
                reason,
            )
            return action

        except Exception:
            logger.exception("Failed to patch HPA hpa=%s", hpa_name)
            return None

    def _current_min(self, hpa_name: str, namespace: Optional[str]) -> int:
        try:
            return self.get_hpa(hpa_name, namespace)["min_replicas"]
        except Exception:
            return 1

    def _build_client(self):
        try:
            from kubernetes import client, config as k8s_config

            if self._cfg.kubeconfig_path:
                k8s_config.load_kube_config(config_file=self._cfg.kubeconfig_path)
            else:
                k8s_config.load_incluster_config()
            return client
        except Exception:
            logger.warning("kubernetes client unavailable — using stub")
            return _StubK8sClient()


class _StubK8sClient:
    """No-op stub used in testing / local dev without a real cluster."""

    class _FakeAPI:
        def read_namespaced_horizontal_pod_autoscaler(self, name, ns):
            from types import SimpleNamespace
            return SimpleNamespace(
                metadata=SimpleNamespace(name=name, namespace=ns),
                spec=SimpleNamespace(min_replicas=1, max_replicas=10),
                status=SimpleNamespace(current_replicas=1, desired_replicas=1),
            )

        def patch_namespaced_horizontal_pod_autoscaler(self, name, ns, body):
            logger.debug("STUB patch HPA %s/%s %s", ns, name, body)

    def AutoscalingV2Api(self):
        return self._FakeAPI()
