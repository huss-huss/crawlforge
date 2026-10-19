"""Token-bucket rate limiting and per-domain politeness throttling."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from crawlforge.clock import Clock, SystemClock

# Float refills can land a hair below 1.0 (e.g. 0.2 s * 5/s); treat that as a full token
# instead of scheduling vanishingly small sleeps that never advance the clock.
_EPSILON = 1e-9


class TokenBucket:
    """Classic token bucket: ``rate`` tokens/second, holding at most ``burst``.

    ``acquire`` waits (via the injected clock) until a token is available.
    Waiters are served in FIFO order thanks to the internal lock.
    """

    def __init__(self, rate: float, burst: float = 1.0, clock: Clock | None = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self.rate = float(rate)
        self.burst = float(burst)
        self._clock = clock or SystemClock()
        self._tokens = self.burst
        self._updated = self._clock.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock.monotonic()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._updated = now

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens

    def time_until(self, tokens: float = 1.0) -> float:
        """Seconds of refill until ``tokens`` tokens have accrued (0 if they are there now).

        The burst cap is ignored: for more than ``burst`` tokens this estimates when the
        callers ahead (who will take tokens as they arrive) are through.
        """
        missing = tokens - self.tokens
        return 0.0 if missing <= _EPSILON else missing / self.rate

    def set_rate(self, rate: float) -> None:
        self._refill()
        self.rate = float(rate)

    async def acquire(self) -> float:
        """Take one token, returning how long the caller waited."""
        waited = 0.0
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= 1.0 - _EPSILON:
                    self._tokens = max(0.0, self._tokens - 1.0)
                    return waited
                wait = (1.0 - self._tokens) / self.rate
                waited += wait
                await self._clock.sleep(wait)


class DomainThrottle:
    """Per-domain concurrency cap + token bucket, created lazily per domain."""

    def __init__(
        self,
        *,
        rate: float,
        burst: float = 1.0,
        concurrency: int = 2,
        clock: Clock | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.rate = rate
        self.burst = burst
        self.concurrency = concurrency
        self._clock = clock or SystemClock()
        self._buckets: dict[str, TokenBucket] = {}
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        # Domains whose bucket ran dry; the only ones (besides those with requests waiting
        # for a token) that ``throttled`` has to look at.
        self._drained: set[str] = set()

    def bucket(self, domain: str) -> TokenBucket:
        if domain not in self._buckets:
            self._buckets[domain] = TokenBucket(self.rate, self.burst, self._clock)
        return self._buckets[domain]

    def semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._semaphores:
            self._semaphores[domain] = asyncio.Semaphore(self.concurrency)
        return self._semaphores[domain]

    def apply_crawl_delay(self, domain: str, delay: float) -> None:
        """Slow a domain down to honour a robots.txt ``Crawl-delay`` (never speeds up)."""
        if delay <= 0:
            return
        bucket = self.bucket(domain)
        bucket.set_rate(min(bucket.rate, 1.0 / delay))

    def throttled(self, reserved: Mapping[str, int]) -> dict[str, float]:
        """Domains without a token for one more request -> seconds until they have one.

        ``reserved`` counts requests per domain that were scheduled but haven't taken their
        token yet; a new request has to wait behind them.
        """
        waits: dict[str, float] = {}
        for domain in self._drained | reserved.keys():
            wait = self.bucket(domain).time_until(1 + reserved.get(domain, 0))
            if wait > 0:
                waits[domain] = wait
            else:
                self._drained.discard(domain)
        return waits

    @asynccontextmanager
    async def slot(self, domain: str) -> AsyncIterator[float]:
        """Hold a concurrency slot for ``domain`` after paying one rate token."""
        async with self.semaphore(domain):
            bucket = self.bucket(domain)
            waited = await bucket.acquire()
            if bucket.time_until() > 0:
                self._drained.add(domain)
            yield waited
