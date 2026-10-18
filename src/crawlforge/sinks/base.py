"""Sink interface shared by every output target."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Self


class Sink(ABC):
    """Receives validated items as JSON-compatible dicts.

    Lifecycle: ``open()`` once, ``write()`` per item, ``close()`` once (which
    must flush anything buffered). Sinks are used from a single event loop,
    so implementations do not need their own locking.
    """

    name: str = "sink"

    async def open(self) -> None:  # noqa: B027 - optional hook
        pass

    @abstractmethod
    async def write(self, item: dict[str, Any]) -> None: ...

    async def flush(self) -> None:  # noqa: B027 - optional hook
        pass

    async def close(self) -> None:
        await self.flush()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
