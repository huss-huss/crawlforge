"""Per-domain circuit breaker."""

from __future__ import annotations

import enum
import math
from collections.abc import Collection

from crawlforge.clock import Clock, SystemClock


class BreakerState(enum.IntEnum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class CircuitBreaker:
    """Stops hammering a failing domain.

    - CLOSED: requests flow; ``failure_threshold`` *consecutive* failures open it.
    - OPEN: requests are refused until ``recovery_timeout`` elapses.
    - HALF_OPEN: exactly one probe request is allowed. Success closes the
      breaker, failure re-opens it for another ``recovery_timeout``.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        clock: Clock | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = clock or SystemClock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> BreakerState:
        if (
            self._state is BreakerState.OPEN
            and self._clock.monotonic() - self._opened_at >= self.recovery_timeout
        ):
            self._state = BreakerState.HALF_OPEN
            self._probe_in_flight = False
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._failures

    def allow(self) -> bool:
        """Return True if a request may be sent now (claims the probe slot when half-open)."""
        state = self.state
        if state is BreakerState.CLOSED:
            return True
        if state is BreakerState.HALF_OPEN and not self._probe_in_flight:
            self._probe_in_flight = True
            return True
        return False

    def retry_in(self) -> float:
        """Seconds until the breaker could admit a request again (0 if it can now)."""
        state = self.state
        if state is BreakerState.OPEN:
            return max(0.0, self.recovery_timeout - (self._clock.monotonic() - self._opened_at))
        if state is BreakerState.HALF_OPEN and self._probe_in_flight:
            # Wait for the probe; poll again shortly.
            return min(1.0, self.recovery_timeout)
        return 0.0

    def release_probe(self) -> None:
        """Give back a half-open probe slot without judging the domain.

        Used when a request fails for reasons unrelated to the server (bad URL,
        robots, a bug in a callback) so the breaker does not stay stuck.
        """
        self._probe_in_flight = False

    def record_success(self) -> None:
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._probe_in_flight = False

    def record_failure(self) -> None:
        state = self.state
        self._failures += 1
        if state is BreakerState.HALF_OPEN or self._failures >= self.failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = self._clock.monotonic()
        self._probe_in_flight = False


class BreakerRegistry:
    """Lazily creates one breaker per domain."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        clock: Clock | None = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = clock or SystemClock()
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, domain: str) -> CircuitBreaker:
        breaker = self._breakers.get(domain)
        if breaker is None:
            breaker = CircuitBreaker(self.failure_threshold, self.recovery_timeout, self._clock)
            self._breakers[domain] = breaker
        return breaker

    def blocked(self, busy: Collection[str] = ()) -> dict[str, float]:
        """Domains whose breaker refuses requests -> seconds until it could admit one.

        ``busy`` are domains with a request in flight. A half-open domain with a request in
        flight is blocked until that request finishes (``inf``): it is, or will claim, the
        probe. A probe claimed with nothing in flight was orphaned and is released.
        """
        blocked: dict[str, float] = {}
        for domain, breaker in self._breakers.items():
            if breaker._state is BreakerState.CLOSED:
                continue
            state = breaker.state
            if state is BreakerState.OPEN:
                blocked[domain] = breaker.retry_in()
            elif domain in busy:
                blocked[domain] = math.inf
            elif breaker._probe_in_flight:
                breaker.release_probe()
        return blocked

    def states(self) -> dict[str, BreakerState]:
        return {domain: b.state for domain, b in self._breakers.items()}
