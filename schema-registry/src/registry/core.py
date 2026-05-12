from __future__ import annotations

from typing import Any

from .compatibility import check_compatibility
from .models import (
    CompatibilityMode,
    CompatibilityResult,
    MigrationScript,
    SchemaVersion,
    SchemaType,
    SubjectConfig,
)
from .storage import Storage


class SchemaRegistry:
    """
    High-level façade over storage + compatibility checking.
    All public methods are async.
    """

    def __init__(self, db_path: str = "registry.db"):
        self.storage = Storage(db_path)

    async def start(self) -> None:
        await self.storage.connect()

    async def stop(self) -> None:
        await self.storage.close()

    # ── Subjects ──────────────────────────────────────────────────────────

    async def list_subjects(self) -> list[str]:
        return await self.storage.get_subjects()

    async def delete_subject(self, subject: str) -> int:
        return await self.storage.delete_subject(subject)

    async def get_config(self, subject: str) -> SubjectConfig:
        cfg = await self.storage.get_subject_config(subject)
        return cfg or SubjectConfig(subject=subject)

    async def set_config(self, subject: str, mode: CompatibilityMode) -> SubjectConfig:
        cfg = SubjectConfig(subject=subject, compatibility=mode)
        await self.storage.upsert_subject_config(cfg)
        return cfg

    # ── Versions ──────────────────────────────────────────────────────────

    async def list_versions(self, subject: str) -> list[int]:
        return await self.storage.get_versions(subject)

    async def get_schema(self, subject: str, version: int | str = "latest") -> SchemaVersion | None:
        if version == "latest":
            return await self.storage.get_latest_schema_version(subject)
        return await self.storage.get_schema_version(subject, int(version))

    async def register_schema(
        self,
        subject: str,
        schema_definition: dict[str, Any],
        schema_type: SchemaType = SchemaType.JSON,
        metadata: dict[str, Any] | None = None,
    ) -> SchemaVersion:
        cfg = await self.get_config(subject)
        existing = await self.storage.get_all_schema_versions(subject)

        # Idempotency: same hash → return existing version
        import hashlib, json
        canonical = json.dumps(schema_definition, sort_keys=True)
        new_hash = hashlib.sha256(canonical.encode()).hexdigest()[:16]
        for ev in existing:
            if ev.schema_hash == new_hash:
                return ev

        # Compatibility check
        compat = check_compatibility(schema_definition, existing, cfg.compatibility)
        if not compat.compatible:
            msgs = "; ".join(e.message for e in compat.errors)
            raise ValueError(f"Schema incompatible ({cfg.compatibility}): {msgs}")

        version = (max((sv.version for sv in existing), default=0)) + 1
        sv = SchemaVersion(
            subject=subject,
            version=version,
            schema_type=schema_type,
            schema_definition=schema_definition,
            metadata=metadata or {},
        )
        return await self.storage.save_schema_version(sv)

    async def delete_version(self, subject: str, version: int) -> bool:
        return await self.storage.delete_version(subject, version)

    # ── Compatibility ─────────────────────────────────────────────────────

    async def check_compatibility(
        self,
        subject: str,
        schema_definition: dict[str, Any],
        mode: CompatibilityMode | None = None,
    ) -> CompatibilityResult:
        cfg = await self.get_config(subject)
        effective_mode = mode or cfg.compatibility
        existing = await self.storage.get_all_schema_versions(subject)
        return check_compatibility(schema_definition, existing, effective_mode)

    # ── Migrations ────────────────────────────────────────────────────────

    async def save_migration(self, script: MigrationScript) -> MigrationScript:
        return await self.storage.save_migration(script)

    async def get_migration(
        self, subject: str, from_version: int, to_version: int
    ) -> MigrationScript | None:
        return await self.storage.get_migration(subject, from_version, to_version)

    async def list_migrations(self, subject: str) -> list[MigrationScript]:
        return await self.storage.get_migrations_for_subject(subject)

    async def build_migration_chain(
        self, subject: str, from_version: int, to_version: int
    ) -> list[MigrationScript]:
        """Find a chain of migrations from → to, using BFS."""
        all_scripts = await self.storage.get_migrations_for_subject(subject)
        graph: dict[int, list[MigrationScript]] = {}
        for s in all_scripts:
            graph.setdefault(s.from_version, []).append(s)

        # BFS
        from collections import deque
        queue: deque[tuple[int, list[MigrationScript]]] = deque([(from_version, [])])
        visited = {from_version}
        while queue:
            current, path = queue.popleft()
            if current == to_version:
                return path
            for edge in graph.get(current, []):
                if edge.to_version not in visited:
                    visited.add(edge.to_version)
                    queue.append((edge.to_version, path + [edge]))
        return []
