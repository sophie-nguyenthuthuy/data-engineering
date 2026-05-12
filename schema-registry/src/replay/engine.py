"""
Event Replay Engine
===================
Re-processes a stream of historical events through a migration chain,
producing events conforming to the target schema version.
"""
from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

import jsonschema

from src.registry.core import SchemaRegistry
from src.registry.models import ReplayResult, TransformEvent
from src.migration.executor import MigrationExecutor
from src.migration.generator import MigrationGenerator


class ReplayEngine:
    def __init__(self, registry: SchemaRegistry):
        self.registry = registry
        self.executor = MigrationExecutor()

    async def replay(
        self,
        subject: str,
        events: list[TransformEvent],
        target_version: int,
        validate: bool = True,
    ) -> ReplayResult:
        """
        Replay events through migration chain to target_version.

        For each event:
          1. Look up migration chain from event.schema_version → target_version
          2. Apply chain sequentially
          3. Optionally validate against target schema
        """
        target_sv = await self.registry.get_schema(subject, target_version)
        if target_sv is None:
            raise ValueError(f"Target schema v{target_version} not found for subject '{subject}'")

        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []

        for event in events:
            try:
                migrated = await self._migrate_event(subject, event, target_version)

                if validate:
                    try:
                        jsonschema.validate(migrated, target_sv.schema_definition)
                    except jsonschema.ValidationError as ve:
                        raise ValueError(f"Validation failed after migration: {ve.message}") from ve

                succeeded.append(
                    {
                        "event_id": event.event_id,
                        "from_version": event.schema_version,
                        "to_version": target_version,
                        "payload": migrated,
                    }
                )
            except Exception as exc:
                failed.append(
                    {
                        "event_id": event.event_id,
                        "from_version": event.schema_version,
                        "error": str(exc),
                    }
                )

        return ReplayResult(
            total=len(events),
            succeeded=len(succeeded),
            failed=len(failed),
            events=succeeded,
            errors=failed,
        )

    async def replay_stream(
        self,
        subject: str,
        events: AsyncIterator[TransformEvent],
        target_version: int,
        validate: bool = True,
    ) -> AsyncIterator[dict[str, Any]]:
        """Async generator variant for streaming large event sets."""
        target_sv = await self.registry.get_schema(subject, target_version)
        if target_sv is None:
            raise ValueError(f"Target schema v{target_version} not found")

        async for event in events:
            try:
                migrated = await self._migrate_event(subject, event, target_version)
                if validate:
                    jsonschema.validate(migrated, target_sv.schema_definition)
                yield {
                    "event_id": event.event_id,
                    "status": "ok",
                    "payload": migrated,
                }
            except Exception as exc:
                yield {"event_id": event.event_id, "status": "error", "error": str(exc)}

    async def _migrate_event(
        self,
        subject: str,
        event: TransformEvent,
        target_version: int,
    ) -> dict[str, Any]:
        if event.schema_version == target_version:
            return dict(event.payload)

        chain = await self.registry.build_migration_chain(
            subject, event.schema_version, target_version
        )

        if not chain:
            # Try to auto-generate migrations for each hop
            chain = await self._auto_generate_chain(subject, event.schema_version, target_version)
            if not chain:
                raise ValueError(
                    f"No migration path from v{event.schema_version} to v{target_version}"
                )

        return self.executor.apply_chain(event.payload, chain)

    async def _auto_generate_chain(
        self,
        subject: str,
        from_version: int,
        to_version: int,
    ) -> list:
        """Walk version-by-version and auto-generate missing migrations."""
        from src.registry.models import MigrationScript

        versions = await self.registry.list_versions(subject)
        try:
            from_idx = versions.index(from_version)
            to_idx = versions.index(to_version)
        except ValueError:
            return []

        if from_idx >= to_idx:
            return []

        hop_versions = versions[from_idx : to_idx + 1]
        chain = []
        gen = MigrationGenerator()

        for i in range(len(hop_versions) - 1):
            v_from = hop_versions[i]
            v_to = hop_versions[i + 1]

            # Check if already stored
            existing = await self.registry.get_migration(subject, v_from, v_to)
            if existing:
                chain.append(existing)
                continue

            # Auto-generate
            sv_from = await self.registry.get_schema(subject, v_from)
            sv_to = await self.registry.get_schema(subject, v_to)
            if sv_from is None or sv_to is None:
                return []

            script = gen.generate(subject, v_from, v_to, sv_from.schema_definition, sv_to.schema_definition)
            saved = await self.registry.save_migration(script)
            chain.append(saved)

        return chain
