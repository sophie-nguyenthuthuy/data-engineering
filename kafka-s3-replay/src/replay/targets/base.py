"""Abstract base class for all replay targets."""

from __future__ import annotations

from abc import ABC, abstractmethod

from replay.models import Event


class BaseTarget(ABC):
    """
    A replay target receives replayed events one at a time.

    Lifecycle:
        await target.open()
        for event in events:
            await target.send(event)
        await target.close()
    """

    @abstractmethod
    async def open(self) -> None:
        """Initialize connections / open files."""
        ...

    @abstractmethod
    async def send(self, event: Event) -> None:
        """Deliver a single event to the downstream system."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Flush buffers and tear down connections."""
        ...

    async def __aenter__(self) -> "BaseTarget":
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
