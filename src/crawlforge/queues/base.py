"""Queue backend interface: a priority queue of requests plus a seen-set."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TypedDict

from crawlforge.http import Request


class FrontierSnapshot(TypedDict):
    pending: list[dict[str, Any]]
    seen: list[str]


class QueueBackend(ABC):
    """Storage for the crawl frontier.

    Higher ``priority`` pops first; equal priorities pop in FIFO order.
    ``add_seen`` must be atomic: it returns ``True`` only for the first caller
    that adds a fingerprint.
    """

    #: True if state survives the process (e.g. Redis), so a crawl can resume
    #: from the backend itself without a checkpoint file.
    persistent: bool = False

    @abstractmethod
    async def push(self, request: Request) -> None: ...

    @abstractmethod
    async def pop(self) -> Request | None: ...

    @abstractmethod
    async def size(self) -> int: ...

    @abstractmethod
    async def add_seen(self, fp: str) -> bool: ...

    @abstractmethod
    async def seen_count(self) -> int: ...

    @abstractmethod
    async def snapshot(self) -> FrontierSnapshot: ...

    @abstractmethod
    async def clear(self) -> None: ...

    async def restore(self, snapshot: FrontierSnapshot) -> None:
        await self.clear()
        for fp in snapshot["seen"]:
            await self.add_seen(fp)
        for data in snapshot["pending"]:
            await self.push(Request.from_dict(data))

    async def close(self) -> None:  # noqa: B027 - optional hook
        pass
