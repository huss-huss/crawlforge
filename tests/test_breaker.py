import math

from crawlforge.breaker import BreakerRegistry, BreakerState, CircuitBreaker
from crawlforge.clock import FakeClock


def make(threshold=3, timeout=10.0):
    clock = FakeClock()
    return CircuitBreaker(threshold, timeout, clock), clock


def test_opens_after_n_consecutive_failures():
    breaker, _ = make(threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is BreakerState.CLOSED and breaker.allow()
    breaker.record_failure()
    assert breaker.state is BreakerState.OPEN
    assert not breaker.allow()


def test_success_resets_consecutive_count():
    breaker, _ = make(threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is BreakerState.CLOSED


def test_half_open_allows_single_probe_then_closes_on_success():
    breaker, clock = make(threshold=1, timeout=10)
    breaker.record_failure()
    assert breaker.retry_in() == 10
    clock.advance(9.9)
    assert breaker.state is BreakerState.OPEN
    clock.advance(0.1)
    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.allow()  # the probe
    assert not breaker.allow()  # nobody else while the probe is in flight
    assert breaker.retry_in() > 0
    breaker.record_success()
    assert breaker.state is BreakerState.CLOSED
    assert breaker.allow() and breaker.allow()


def test_failed_probe_reopens_for_full_timeout():
    breaker, clock = make(threshold=2, timeout=5)
    breaker.record_failure()
    breaker.record_failure()
    clock.advance(5)
    assert breaker.allow()
    breaker.record_failure()
    assert breaker.state is BreakerState.OPEN
    assert breaker.retry_in() == 5
    clock.advance(5)
    assert breaker.state is BreakerState.HALF_OPEN


def test_registry_is_per_domain():
    registry = BreakerRegistry(failure_threshold=1, recovery_timeout=1, clock=FakeClock())
    registry.get("a.test").record_failure()
    assert registry.get("a.test").state is BreakerState.OPEN
    assert registry.get("b.test").state is BreakerState.CLOSED
    assert registry.states() == {"a.test": BreakerState.OPEN, "b.test": BreakerState.CLOSED}


def test_registry_reports_blocked_domains():
    clock = FakeClock()
    registry = BreakerRegistry(failure_threshold=1, recovery_timeout=10, clock=clock)
    registry.get("ok.test")
    registry.get("down.test").record_failure()
    assert registry.blocked() == {"down.test": 10}
    clock.advance(4)
    assert registry.blocked() == {"down.test": 6}
    clock.advance(6)
    # Half-open: free for its probe, unless a request of the domain is already in flight.
    assert registry.blocked() == {}
    assert registry.blocked(busy={"down.test"}) == {"down.test": math.inf}
    # A probe claimed with nothing in flight was orphaned; it is released.
    assert registry.get("down.test").allow()
    assert registry.blocked() == {}
    assert registry.get("down.test").allow()
