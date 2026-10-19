"""The frontier: de-duplication and depth limits in front of a queue backend."""

from __future__ import annotations

import enum

from crawlforge.http import Request
from crawlforge.queues.base import Delivery, QueueBackend


class PushResult(enum.Enum):
    QUEUED = "queued"
    DUPLICATE = "duplicate"
    TOO_DEEP = "too_deep"


class Frontier:
    def __init__(self, backend: QueueBackend, max_depth: int | None = None) -> None:
        self.backend = backend
        self.max_depth = max_depth

    async def push(self, request: Request, *, force: bool = False) -> PushResult:
        """Queue ``request`` unless it was already seen or is too deep.

        ``force`` re-queues without any checks; the engine uses it for retries
        and deferred requests whose fingerprint is (by design) already seen.
        """
        if not force:
            if self.max_depth is not None and request.depth > self.max_depth:
                return PushResult.TOO_DEEP
            if not request.dont_filter and not await self.backend.add_seen(request.fingerprint):
                return PushResult.DUPLICATE
        await self.backend.push(request)
        return PushResult.QUEUED

    async def pop(self) -> Request | None:
        return await self.backend.pop()

    async def receive(self, lease_ttl: float) -> Delivery | None:
        """Pop for processing; leasing backends hold the request until it is settled."""
        return await self.backend.receive(lease_ttl)

    async def size(self) -> int:
        return await self.backend.size()
