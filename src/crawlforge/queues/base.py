"""Queue backend interface: per-domain priority queues of requests plus a seen-set."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from typing import Any, TypedDict

from crawlforge.http import Request


class FrontierSnapshot(TypedDict):
    pending: list[dict[str, Any]]
    seen: list[str]


@dataclass(slots=True)
class Delivery:
    """A popped request. ``lease`` is set by leasing backends and must be settled.

    ``deliveries`` counts how often this queue entry was handed out (1 on the first pop);
    it only grows when a lease expires and the entry is reclaimed.
    """

    request: Request
    lease: str | None = None
    deliveries: int = 1


class QueueBackend(ABC):
    """Storage for the crawl frontier, kept as one queue per domain.

    Within a domain, higher ``priority`` pops first and equal priorities pop in FIFO order.
    Across domains, a pop serves the domain whose next request has the highest priority;
    domains tied on priority take turns (round-robin). ``skip`` names domains the caller
    can't send to right now (throttled, at their concurrency cap, breaker open): their
    requests stay queued, in place, and the next domain is served instead.

    ``add_seen`` must be atomic: it returns ``True`` only for the first caller
    that adds a fingerprint.
    """

    #: True if state survives the process (e.g. Redis), so a crawl can resume
    #: from the backend itself without a checkpoint file.
    persistent: bool = False
    #: True if ``receive`` hands out leases that must be acked, released or re-queued
    #: (several processes can then share the queue). Non-leasing backends treat a pop as
    #: final and implement the lease methods below as no-ops.
    leasing: bool = False

    @abstractmethod
    async def push(self, request: Request) -> None: ...

    @abstractmethod
    async def pop(self, skip: Collection[str] = ()) -> Request | None:
        """Pop the next request of a domain not in ``skip`` (None if there is none)."""

    @abstractmethod
    async def queued_domains(self, domains: Iterable[str]) -> set[str]:
        """The subset of ``domains`` that have queued requests."""

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

    # Lease protocol. The defaults describe a backend without leases: ``receive`` is a
    # plain pop, ``requeue`` a plain push, and there is never anything in flight.

    async def receive(self, lease_ttl: float, skip: Collection[str] = ()) -> Delivery | None:
        """Pop like ``pop``; leasing backends hold the request in flight for ``lease_ttl`` s."""
        request = await self.pop(skip)
        return None if request is None else Delivery(request)

    async def ack(self, lease: str) -> bool:
        """Finish a leased request. False if the lease was lost (expired and reclaimed)."""
        return True

    async def requeue(self, lease: str | None, request: Request) -> bool:
        """Finish ``lease`` and queue ``request`` (e.g. a retry) in one step.

        Returns False, and queues nothing, if the lease was lost: the reclaimed entry is
        already back in the queue.
        """
        await self.push(request)
        return True

    async def release(self, leases: Iterable[str]) -> int:
        """Put leased requests back unprocessed (shutdown); doesn't count as a delivery."""
        return 0

    async def extend(self, leases: Iterable[str], lease_ttl: float) -> list[str]:
        """Push lease deadlines out by ``lease_ttl``. Returns the leases that were lost."""
        return []

    async def reclaim(self) -> int:
        """Move expired leases (from any process) back to the queue; returns how many."""
        return 0

    async def in_flight(self) -> int:
        """Leased requests across all processes."""
        return 0
