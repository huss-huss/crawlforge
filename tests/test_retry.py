import random
from email.utils import formatdate

import httpx
import pytest

from crawlforge.retry import RetryPolicy, is_transient_exception, parse_retry_after


def test_backoff_is_bounded_exponential_with_full_jitter():
    policy = RetryPolicy(backoff_base=1.0, backoff_cap=10.0, rng=random.Random(42))
    for attempt, ceiling in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 10), (9, 10)]:
        samples = [policy.backoff(attempt) for _ in range(200)]
        assert all(0 <= s <= ceiling for s in samples)
        # Jitter actually spreads values out across the window.
        assert max(samples) > ceiling * 0.8
        assert min(samples) < ceiling * 0.2


def test_backoff_is_deterministic_with_seeded_rng():
    a = RetryPolicy(rng=random.Random(7))
    b = RetryPolicy(rng=random.Random(7))
    assert [a.backoff(n) for n in range(1, 6)] == [b.backoff(n) for n in range(1, 6)]


def test_retry_after_overrides_backoff_and_is_capped():
    policy = RetryPolicy(max_retry_after=60)
    assert policy.delay(1, retry_after=12.0) == 12.0
    assert policy.delay(1, retry_after=3600.0) == 60.0


def test_parse_retry_after_seconds_and_http_date():
    now = 1_700_000_000.0
    assert parse_retry_after("120", now) == 120.0
    assert parse_retry_after(formatdate(now + 30, usegmt=True), now) == pytest.approx(30, abs=1)
    assert parse_retry_after(formatdate(now - 30, usegmt=True), now) == 0.0
    assert parse_retry_after("soon", now) is None
    assert parse_retry_after(None, now) is None


def test_status_and_exception_classification():
    policy = RetryPolicy(max_retries=2)
    for status in (429, 500, 502, 503, 504):
        assert policy.should_retry_status(status)
    for status in (200, 301, 400, 403, 404):
        assert not policy.should_retry_status(status)
    assert policy.can_retry(0) and policy.can_retry(1) and not policy.can_retry(2)
    assert is_transient_exception(httpx.ConnectError("boom"))
    assert is_transient_exception(httpx.ReadTimeout("slow"))
    assert not is_transient_exception(ValueError("bug"))
