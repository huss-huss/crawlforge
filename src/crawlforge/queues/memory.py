"""In-process backend: one priority heap per domain plus an index of domains with work."""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Collection, Iterable

from crawlforge.http import Request
from crawlforge.queues.base import FrontierSnapshot, QueueBackend

# (-priority, insertion seq, request): heap order is priority desc, FIFO.
_Entry = tuple[int, int, Request]
# A domain's place in the index: (-priority of its next request, turn).
_Key = tuple[int, int]


class MemoryQueue(QueueBackend):
    persistent = False

    def __init__(self) -> None:
        self._domains: dict[str, list[_Entry]] = {}
        # Domains with queued requests, best first. Entries go stale when a domain is
        # re-keyed; ``_keys`` holds the live key and stale entries are dropped lazily.
        self._index: list[tuple[int, int, str]] = []
        self._keys: dict[str, _Key] = {}
        self._seq = itertools.count()
        # A domain takes a new turn each time it is served, which puts it behind the other
        # domains with the same head priority (round-robin).
        self._turns = itertools.count()
        self._size = 0
        self._seen: set[str] = set()

    def _set_key(self, domain: str, key: _Key) -> None:
        self._keys[domain] = key
        heapq.heappush(self._index, (*key, domain))

    async def push(self, request: Request) -> None:
        domain = request.domain
        entry = (-request.priority, next(self._seq), request)
        heap = self._domains.setdefault(domain, [])
        heapq.heappush(heap, entry)
        self._size += 1
        key = self._keys.get(domain)
        if key is None:
            self._set_key(domain, (entry[0], next(self._turns)))
        elif entry[0] < key[0]:
            # A better request jumped the domain's queue; the domain keeps its turn.
            self._set_key(domain, (entry[0], key[1]))

    async def pop(self, skip: Collection[str] = ()) -> Request | None:
        passed: list[tuple[int, int, str]] = []
        request = None
        while self._index:
            item = heapq.heappop(self._index)
            prio, turn, domain = item
            if self._keys.get(domain) != (prio, turn):
                continue  # stale
            if domain in skip:
                passed.append(item)
                continue
            heap = self._domains[domain]
            request = heapq.heappop(heap)[2]
            self._size -= 1
            if heap:
                self._set_key(domain, (heap[0][0], next(self._turns)))
            else:
                del self._domains[domain], self._keys[domain]
            break
        for item in passed:
            heapq.heappush(self._index, item)
        return request

    async def queued_domains(self, domains: Iterable[str]) -> set[str]:
        return {d for d in domains if d in self._domains}

    async def size(self) -> int:
        return self._size

    async def add_seen(self, fp: str) -> bool:
        if fp in self._seen:
            return False
        self._seen.add(fp)
        return True

    async def seen_count(self) -> int:
        return len(self._seen)

    async def snapshot(self) -> FrontierSnapshot:
        ordered = sorted(
            (entry for heap in self._domains.values() for entry in heap),
            key=lambda entry: (entry[0], entry[1]),
        )
        return {
            "pending": [req.to_dict() for _, _, req in ordered],
            "seen": sorted(self._seen),
        }

    async def clear(self) -> None:
        self._domains.clear()
        self._index.clear()
        self._keys.clear()
        self._size = 0
        self._seen.clear()
