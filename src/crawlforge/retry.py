"""Retry classification and exponential back-off with jitter."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

import httpx

RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

# Network-level failures that are worth another attempt. Note that
# ``httpx.TimeoutException`` and ``httpx.NetworkError`` are subclasses of
# ``httpx.TransportError``; protocol/decoding errors usually are not transient.
TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    TimeoutError,
    ConnectionError,
)


def is_transient_exception(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_EXCEPTIONS)


def parse_retry_after(value: str | None, now: float) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds.

    ``now`` is the current wall-clock UNIX time. Returns ``None`` when the
    header is missing or malformed; never returns a negative number.
    """
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    return max(0.0, when.timestamp() - now)


@dataclass
class RetryPolicy:
    """Decides whether and when to retry.

    Delay for attempt ``n`` (1-based) uses "full jitter":
    ``uniform(0, min(cap, base * 2 ** (n - 1)))``. A server-provided
    ``Retry-After`` takes precedence (bounded by ``max_retry_after``) because
    the server knows better than our guess.
    """

    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_cap: float = 30.0
    max_retry_after: float = 300.0
    retry_statuses: frozenset[int] = RETRY_STATUSES
    rng: random.Random = field(default_factory=random.Random, repr=False)

    def should_retry_status(self, status: int) -> bool:
        return status in self.retry_statuses

    def can_retry(self, attempts_so_far: int) -> bool:
        return attempts_so_far < self.max_retries

    def backoff(self, attempt: int) -> float:
        ceiling = min(self.backoff_cap, self.backoff_base * (2 ** max(0, attempt - 1)))
        return self.rng.uniform(0, ceiling)

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        if retry_after is not None:
            return min(retry_after, self.max_retry_after)
        return self.backoff(attempt)
