from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseDestination

logger = get_logger(__name__)


class SalesforceDestination(BaseDestination):
    """
    Upserts records into a Salesforce object.

    params:
        object_name (str): Salesforce object API name, e.g. "Contact", "Opportunity"
        operation (str): "upsert" | "insert" | "update"
        external_id_field (str): Required for upsert, e.g. "Email"
        batch_size (int): Records per Salesforce bulk batch (default 200)
    """

    def __init__(self, params: dict[str, Any]) -> None:
        from simple_salesforce import Salesforce  # lazy import — optional dependency
        super().__init__(params)
        self._object_name = params["object_name"]
        self._operation = params.get("operation", "upsert")
        self._external_id_field = params.get("external_id_field", "Id")
        self._batch_size = int(params.get("batch_size", 200))
        self._sf = Salesforce(
            username=params.get("username", settings.salesforce_username),
            password=params.get("password", settings.salesforce_password),
            security_token=params.get("security_token", settings.salesforce_security_token),
            domain=params.get("domain", settings.salesforce_domain),
        )

    def send(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        obj = getattr(self._sf, self._object_name)
        synced = 0

        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            results = []

            if self._operation == "upsert":
                results = obj.upsert(
                    [{"type": self._object_name, **r} for r in batch],
                    external_id_field=self._external_id_field,
                )
            elif self._operation == "insert":
                results = obj.insert(batch)
            elif self._operation == "update":
                results = obj.update(batch)

            batch_ok = sum(1 for r in results if r.get("success", False))
            synced += batch_ok
            logger.debug(f"SalesforceDestination: batch {i//self._batch_size + 1} → {batch_ok}/{len(batch)} ok")

        logger.info(f"SalesforceDestination: synced {synced}/{len(records)} records to {self._object_name}")
        return synced
