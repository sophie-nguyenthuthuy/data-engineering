"""
Schema evolution handler using Confluent Schema Registry.

Supports:
  - Field additions with defaults (BACKWARD compatible)
  - Field removals (FORWARD compatible — ignored on read)
  - Field renames via Avro aliases
  - Version tracking per subject

Compatibility mode is set to BACKWARD in Schema Registry config, meaning:
  new schema readers can read data written with old schemas.
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer

log = logging.getLogger(__name__)


# Maps Avro field types to Python default values when a field is missing
_AVRO_TYPE_DEFAULTS: Dict[str, Any] = {
    "null":    None,
    "string":  None,
    "int":     None,
    "long":    None,
    "float":   None,
    "double":  None,
    "boolean": None,
    "bytes":   None,
}


def _parse_avro_schema(schema_str: str) -> Dict:
    return json.loads(schema_str)


def _field_map(schema: Dict) -> Dict[str, Dict]:
    """Returns {field_name: field_def, ...alias: field_def, ...}"""
    fields = {}
    for f in schema.get("fields", []):
        fields[f["name"]] = f
        for alias in f.get("aliases", []):
            fields[alias] = f
    return fields


class SchemaEvolutionHandler:
    """
    Detects schema version changes and migrates records to the latest schema.

    Usage:
        handler = SchemaEvolutionHandler("http://schema-registry:8081")
        migrated = handler.migrate(record, schema_id, subject="cdc.public.users-value")
    """

    def __init__(self, schema_registry_url: str):
        self._client = SchemaRegistryClient({"url": schema_registry_url})
        self._schema_cache: Dict[int, Dict] = {}        # schema_id -> parsed schema
        self._latest_cache: Dict[str, Tuple[int, Dict]] = {}  # subject -> (schema_id, schema)
        self._version_log: Dict[str, int] = {}           # subject -> last seen schema_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def migrate(self, record: dict, schema_id: int, subject: str) -> dict:
        """
        Migrate `record` (written with `schema_id`) to the latest schema for `subject`.
        Returns the record unchanged if already at latest; applies defaults for new fields.
        """
        source_schema = self._get_schema(schema_id)
        latest_id, latest_schema = self._get_latest(subject)

        if schema_id == latest_id:
            return record

        if schema_id not in self._version_log or self._version_log[subject] != latest_id:
            log.info("Schema evolution detected on %s: %d → %d", subject, schema_id, latest_id)
            self._version_log[subject] = latest_id

        return self._apply_migration(record, source_schema, latest_schema)

    def get_latest_schema_id(self, subject: str) -> int:
        schema_id, _ = self._get_latest(subject)
        return schema_id

    def invalidate_latest_cache(self, subject: str) -> None:
        self._latest_cache.pop(subject, None)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_schema(self, schema_id: int) -> Dict:
        if schema_id not in self._schema_cache:
            schema = self._client.get_schema(schema_id)
            self._schema_cache[schema_id] = _parse_avro_schema(schema.schema_str)
        return self._schema_cache[schema_id]

    def _get_latest(self, subject: str) -> Tuple[int, Dict]:
        if subject not in self._latest_cache:
            registered = self._client.get_latest_version(subject)
            parsed = _parse_avro_schema(registered.schema.schema_str)
            self._latest_cache[subject] = (registered.schema_id, parsed)
        return self._latest_cache[subject]

    def _apply_migration(self, record: dict, from_schema: Dict, to_schema: Dict) -> dict:
        """
        Project `record` onto `to_schema`:
          - Carry forward fields present in record (using aliases for renames)
          - Fill missing fields with their default values
          - Drop fields not in to_schema
        """
        from_fields = _field_map(from_schema)
        migrated: dict = {}

        for field_def in to_schema.get("fields", []):
            name = field_def["name"]
            aliases = field_def.get("aliases", [])

            # Try the canonical name first, then any alias
            candidates = [name] + aliases
            value_found = False
            for candidate in candidates:
                if candidate in record:
                    migrated[name] = record[candidate]
                    value_found = True
                    break

            if not value_found:
                if "default" in field_def:
                    migrated[name] = field_def["default"]
                else:
                    # No default and missing — log and use type-based null
                    field_type = field_def.get("type", "null")
                    base_type = field_type[0] if isinstance(field_type, list) else field_type
                    migrated[name] = _AVRO_TYPE_DEFAULTS.get(base_type, None)
                    log.warning(
                        "Field '%s' missing in source record and has no default; using null", name
                    )

        return migrated
