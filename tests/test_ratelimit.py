import asyncio

import pytest

from crawlforge.clock import FakeClock
from crawlforge.ratelimit import DomainThrottle, TokenBucket


async def test_bucket_paces_requests_at_rate():
    clock = FakeClock()
    bucket = TokenBucket(rate=5, burst=1, clock=clock)
    for _ in range(10):
        await bucket.acquire()
    # First token is free (bucket starts full), the remaining 9 cost 1/5 s each.
    assert clock.monotonic() == pytest.approx(9 / 5)


async def test_burst_allows_initial_spike_then_paces():
    clock = FakeClock()
    bucket = TokenBucket(rate=2, burst=4, clock=clock)
    for _ in range(4):
        assert await bucket.acquire() == 0
    assert clock.monotonic() == 0
    waited = await bucket.acquire()
    assert waited == pytest.approx(0.5)


async def test_bucket_refills_while_idle_but_caps_at_burst():
    clock = FakeClock()
    bucket = TokenBucket(rate=1, burst=3, clock=clock)
    for _ in range(3):
        await bucket.acquire()
    clock.advance(100)
    assert bucket.tokens == pytest.approx(3)


async def test_concurrent_waiters_are_serialized():
    clock = FakeClock()
    bucket = TokenBucket(rate=10, burst=1, clock=clock)
    await asyncio.gather(*(bucket.acquire() for _ in range(6)))
    assert clock.monotonic() == pytest.approx(0.5)


def test_invalid_parameters():
    with pytest.raises(ValueError):
        TokenBucket(rate=0)
    with pytest.raises(ValueError):
        TokenBucket(rate=1, burst=0.5)


async def test_domain_throttle_isolates_domains_and_caps_concurrency():
    clock = FakeClock()
    throttle = DomainThrottle(rate=1, burst=1, concurrency=2, clock=clock)
    active = 0
    peak = 0

    async def hit(domain):
        nonlocal active, peak
        async with throttle.slot(domain):
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            active -= 1

    await asyncio.gather(*(hit("a.test") for _ in range(5)))
    assert peak <= 2
    assert throttle.bucket("a.test") is not throttle.bucket("b.test")
    # b.test has its own full bucket, so it does not wait.
    async with throttle.slot("b.test") as waited:
        assert waited == 0


def test_crawl_delay_only_slows_down():
    throttle = DomainThrottle(rate=4, burst=1, clock=FakeClock())
    throttle.apply_crawl_delay("a.test", 2.0)
    assert throttle.bucket("a.test").rate == pytest.approx(0.5)
    throttle.apply_crawl_delay("a.test", 0.01)
    assert throttle.bucket("a.test").rate == pytest.approx(0.5)
