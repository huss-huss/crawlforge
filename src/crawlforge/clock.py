"""Injectable clocks so time-dependent logic can be tested deterministically."""

from __future__ import annotations

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def time(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class FakeClock:
    """A clock whose ``sleep`` advances virtual time instantly.

    ``sleeps`` records every requested sleep so tests can assert on back-off
    schedules without waiting in real time.
    """

    def __init__(self, start: float = 0.0, wall: float = 1_700_000_000.0) -> None:
        self._now = start
        self._wall_offset = wall - start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self._now

    def time(self) -> float:
        return self._now + self._wall_offset

    def advance(self, seconds: float) -> None:
        self._now += seconds

    async def sleep(self, seconds: float) -> None:
        seconds = max(0.0, seconds)
        self.sleeps.append(seconds)
        self._now += seconds
        await asyncio.sleep(0)
