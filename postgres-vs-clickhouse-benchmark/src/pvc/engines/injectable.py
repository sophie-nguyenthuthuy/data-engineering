"""Engine adapter over an injectable ``execute(sql) -> rows`` callable.

This is the production wedge: real Postgres / ClickHouse drivers are
optional dependencies that vary across deployments, so we don't pin
them. The user supplies ``execute_fn`` (e.g. wired to psycopg2 or
clickhouse-driver) and the harness gets the same uniform interface as
every other engine.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pvc.engines.base import Engine

ExecuteFn = Callable[[str], list[tuple[Any, ...]]]


@dataclass
class InjectableEngine(Engine):
    """Wraps a user-supplied executor."""

    execute_fn: ExecuteFn
    name: str = "injectable"
    ddl_runner: ExecuteFn | None = None
    closer: Callable[[], None] | None = None
    _is_setup: bool = field(default=False, init=False, repr=False)

    def setup(self, ddl: list[str], inserts: list[tuple[str, list[tuple[Any, ...]]]]) -> None:
        runner = self.ddl_runner or self.execute_fn
        for stmt in ddl:
            runner(stmt)
        # Inserts are executed row-by-row via the same fn so the runner
        # remains a single uniform contract; production callers swap in
        # an executemany-aware fn if they need bulk speed.
        for sql, rows in inserts:
            for row in rows:
                # Substitute positional placeholders with literals — this
                # adapter is for tests / demos; production swaps in a
                # parameterised runner.
                values = ", ".join(_lit(v) for v in row)
                runner(sql.replace("?", values))
        self._is_setup = True

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        if not self._is_setup:
            from pvc.engines.base import EngineError

            raise EngineError("engine not set up")
        return list(self.execute_fn(sql))

    def close(self) -> None:
        if self.closer is not None:
            self.closer()


def _lit(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


__all__ = ["ExecuteFn", "InjectableEngine"]
