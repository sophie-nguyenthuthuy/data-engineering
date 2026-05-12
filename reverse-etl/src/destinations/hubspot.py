from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseDestination

logger = get_logger(__name__)


class HubSpotDestination(BaseDestination):
    """
    Upserts contacts (or companies) in HubSpot.

    params:
        object_type (str): "contacts" | "companies" | "deals" (default: "contacts")
        id_property (str): Property to use for dedup/upsert (default: "email")
        batch_size (int): Records per API call (max 100 per HubSpot limits)
    """

    def __init__(self, params: dict[str, Any]) -> None:
        import hubspot  # lazy import — optional dependency
        super().__init__(params)
        token = params.get("access_token", settings.hubspot_access_token)
        self._client = hubspot.Client.create(access_token=token)
        self._object_type = params.get("object_type", "contacts")
        self._id_property = params.get("id_property", "email")
        self._batch_size = min(int(params.get("batch_size", 100)), 100)

    def _upsert_batch(self, batch: list[dict[str, Any]]) -> int:
        from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInputUpsert  # type: ignore
        from hubspot.crm.contacts.models import SimplePublicObjectBatchInputUpsert  # type: ignore

        inputs = [
            SimplePublicObjectBatchInputUpsert(
                id_property=self._id_property,
                id=str(rec.get(self._id_property, "")),
                properties={k: str(v) for k, v in rec.items()},
            )
            for rec in batch
            if rec.get(self._id_property)
        ]

        if not inputs:
            return 0

        api = getattr(self._client.crm, self._object_type).batch_api
        result = api.upsert(
            batch_input_simple_public_object_batch_input_upsert=BatchInputSimplePublicObjectBatchInputUpsert(
                inputs=inputs
            )
        )
        return len(result.results)

    def send(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        synced = 0
        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            try:
                ok = self._upsert_batch(batch)
                synced += ok
            except ApiException as e:
                logger.error(f"HubSpotDestination: batch {i//self._batch_size + 1} failed: {e}")

        logger.info(f"HubSpotDestination: synced {synced}/{len(records)} to {self._object_type}")
        return synced
