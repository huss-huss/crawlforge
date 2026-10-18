"""In-process priority queue backend."""

from __future__ import annotations

import heapq
import itertools

from crawlforge.http import Request
from crawlforge.queues.base import FrontierSnapshot, QueueBackend


class MemoryQueue(QueueBackend):
    persistent = False

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, Request]] = []
        self._seq = itertools.count()
        self._seen: set[str] = set()

    async def push(self, request: Request) -> None:
        heapq.heappush(self._heap, (-request.priority, next(self._seq), request))

    async def pop(self) -> Request | None:
        if not self._heap:
            return None
        return heapq.heappop(self._heap)[2]

    async def size(self) -> int:
        return len(self._heap)

    async def add_seen(self, fp: str) -> bool:
        if fp in self._seen:
            return False
        self._seen.add(fp)
        return True

    async def seen_count(self) -> int:
        return len(self._seen)

    async def snapshot(self) -> FrontierSnapshot:
        ordered = sorted(self._heap, key=lambda entry: (entry[0], entry[1]))
        return {
            "pending": [req.to_dict() for _, _, req in ordered],
            "seen": sorted(self._seen),
        }

    async def clear(self) -> None:
        self._heap.clear()
        self._seen.clear()
