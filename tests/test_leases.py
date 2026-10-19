"""Lease-based Redis pops: several crawlers sharing one frontier (no network).

Every test runs against fakeredis and, when ``TEST_REDIS_URL`` is set, a real Redis
(the ``make_queue`` fixture in conftest.py).
"""

import asyncio
import time
from collections import Counter

import pytest

from crawlforge import Request, Spider
from crawlforge.clock import SystemClock
from crawlforge.frontier import Frontier
from crawlforge.queues.redis import RedisQueue
from crawlforge.replay import queue_pending
from test_engine import html, read_jsonl

PAGES = 40


async def eventually(predicate, timeout=5.0):
    for _ in range(int(timeout / 0.005)):
        if await predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met in time")


def past(seconds=3600.0):
    """A wall clock running behind: leases it hands out are already expired for others."""
    return lambda: time.time() - seconds


async def leased_entries(queue):
    """Raw delivery counters by URL (queued or leased entries only)."""
    counts = await queue._redis.hgetall(queue.deliveries_key)
    return {RedisQueue._decode(m).url: int(n) for m, n in counts.items()}


async def _at_least(value, n):
    return await value >= n


async def _truthy(value):
    return bool(value)


# ---------------------------------------------------------------------- queue level


async def test_receive_leases_until_ack(make_queue):
    queue = make_queue()
    await queue.push(Request("http://x.test/low"))
    await queue.push(Request("http://x.test/high", priority=5))

    first = await queue.receive(lease_ttl=30)
    assert first.request.url == "http://x.test/high" and first.deliveries == 1
    assert await queue.size() == 1 and await queue.in_flight() == 1
    # Another process sees the lease, not a second copy of the request.
    other = make_queue()
    assert (await other.receive(lease_ttl=30)).request.url == "http://x.test/low"
    assert await other.receive(lease_ttl=30) is None
    assert await other.in_flight() == 2

    assert await queue.ack(first.lease)
    assert not await queue.ack(first.lease)  # acks are idempotent
    assert await queue.in_flight() == 1
    assert await leased_entries(queue) == {"http://x.test/low": 1}


async def test_concurrent_receives_never_share_a_request(make_queue):
    a, b = make_queue(), make_queue()
    for i in range(50):
        await a.push(Request(f"http://x.test/{i}"))
    results = await asyncio.gather(*(q.receive(30) for q in [a, b] * 40))
    urls = [d.request.url for d in results if d is not None]
    assert len(urls) == 50 and len(set(urls)) == 50
    assert await a.size() == 0 and await a.in_flight() == 50


async def test_expired_lease_is_reclaimed_with_its_score_and_delivery_count(make_queue):
    crashed, survivor = make_queue(clock=past()), make_queue()
    await survivor.push(Request("http://x.test/first", priority=1))
    await survivor.push(Request("http://x.test/second", priority=1))
    lost = await crashed.receive(lease_ttl=30)
    held = await survivor.receive(lease_ttl=30)
    assert lost.request.url == "http://x.test/first"

    assert await survivor.reclaim() == 1  # only the expired one
    assert await survivor.size() == 1 and await survivor.in_flight() == 1
    await survivor.push(Request("http://x.test/third", priority=1))
    again = await survivor.receive(lease_ttl=30)
    # Back at its original place in the queue (ahead of later pushes), second delivery.
    assert again.request == lost.request and again.deliveries == 2

    # The crashed worker comes back: its lease is gone, so it can't settle the request.
    assert await crashed.extend([lost.lease, held.lease], 30) == [lost.lease]
    assert not await crashed.ack(lost.lease)
    assert not await crashed.requeue(lost.lease, lost.request)
    assert await survivor.size() == 1  # nothing was queued by the stale requeue
    assert await survivor.ack(again.lease) and await survivor.ack(held.lease)
    assert await survivor.in_flight() == 0


async def test_extended_lease_is_not_reclaimed(make_queue):
    now = [1000.0]
    queue = make_queue(clock=lambda: now[0])
    await queue.push(Request("http://x.test/slow"))
    delivery = await queue.receive(lease_ttl=10)
    now[0] += 8
    assert await queue.extend([delivery.lease], 10) == []
    now[0] += 8  # 16 s after the pop, but only 8 s after the extension
    assert await queue.reclaim() == 0
    now[0] += 3
    assert await queue.reclaim() == 1


async def test_release_requeues_without_counting_a_delivery(make_queue):
    queue = make_queue()
    await queue.push(Request("http://x.test/a"))
    first = await queue.receive(30)
    assert await queue.release([first.lease, "unknown"]) == 1
    assert await queue.size() == 1 and await queue.in_flight() == 0
    assert await leased_entries(queue) == {}
    assert (await queue.receive(30)).deliveries == 1


async def test_requeue_swaps_a_lease_for_a_new_request(make_queue):
    queue = make_queue()
    await queue.push(Request("http://x.test/a"))
    delivery = await queue.receive(30)
    delivery.request.retries = 1
    assert await queue.requeue(delivery.lease, delivery.request)
    assert await queue.in_flight() == 0
    retry = await queue.receive(30)
    assert retry.request.retries == 1 and retry.deliveries == 1


async def test_snapshot_includes_leased_requests_and_clear_drops_leases(make_queue):
    queue = make_queue()
    frontier = Frontier(queue)
    for i, prio in enumerate([0, 3, 1]):
        await frontier.push(Request(f"http://x.test/{i}", priority=prio))
    await queue.receive(30)  # x.test/1, the highest priority
    snap = await queue.snapshot()
    assert [p["url"] for p in snap["pending"]] == [
        "http://x.test/1",
        "http://x.test/2",
        "http://x.test/0",
    ]
    assert await queue_pending(queue) == {
        Request(f"http://x.test/{i}").fingerprint for i in range(3)
    }
    await queue.clear()
    assert await queue.in_flight() == 0 and await queue.snapshot() == {"pending": [], "seen": []}


# ---------------------------------------------------------------------- engines


class LinkedSpider(Spider):
    """A start page linking to PAGES products; every product links to the next and home."""

    name = "linked"
    start_urls = ["http://shop.test/"]

    def parse(self, response):
        for i in range(PAGES):
            yield response.follow(f"/p/{i}", callback=self.parse_product)

    def parse_product(self, response):
        i = int(response.url.rsplit("/", 1)[1])
        yield {"page": i}
        yield response.follow(f"/p/{(i + 1) % PAGES}", callback=self.parse_product)
        yield response.follow("/", callback=self.parse)


def counting_handler(hits, *, delay=0.002, slow=None):
    async def handler(request):
        hits[request.url.path] += 1
        await asyncio.sleep(slow.get(request.url.path, delay) if slow else delay)
        return html("<p>ok</p>")

    return handler


async def seed(queue, *urls):
    frontier = Frontier(queue)
    for url in urls:
        await frontier.push(Request(url))


def engine(make_crawler, tmp_path, name, handler, queue, **overrides):
    return make_crawler(
        LinkedSpider(),
        handler,
        queue=queue,
        resume=True,
        output_dir=tmp_path / name,
        sink_path=tmp_path / name / "items.jsonl",
        concurrency=2,
        **overrides,
    )


async def test_two_engines_share_one_frontier_exactly_once(make_crawler, make_queue, tmp_path):
    await seed(make_queue(), "http://shop.test/")
    hits = Counter()
    crawlers = [
        engine(make_crawler, tmp_path, name, counting_handler(hits), make_queue())
        for name in ("a", "b")
    ]
    stats = await asyncio.gather(*(c.run() for c in crawlers))

    assert len(hits) == PAGES + 1 and set(hits.values()) == {1}
    assert all(s.requests > 0 for s in stats), [s.requests for s in stats]
    assert sum(s.requests for s in stats) == PAGES + 1
    assert all(s.finish_reason == "finished" for s in stats)
    assert all(s.leases_lost == 0 and s.leases_reclaimed == 0 for s in stats)
    pages = [i["page"] for c in crawlers for i in read_jsonl(c.test_sink_path)]
    assert sorted(pages) == list(range(PAGES))
    queue = make_queue()
    assert await queue.size() == 0 and await queue.in_flight() == 0
    assert await queue.seen_count() == PAGES + 1


async def test_crashed_workers_lease_expires_and_another_worker_takes_over(
    make_crawler, make_queue, tmp_path
):
    queue = make_queue()
    await seed(queue, "http://shop.test/")
    crashed = make_queue(clock=past())
    taken = await crashed.receive(lease_ttl=60)  # popped, then the process "dies"
    assert taken.request.url == "http://shop.test/" and await queue.size() == 0

    hits = Counter()
    survivor = engine(make_crawler, tmp_path, "b", counting_handler(hits, delay=0), make_queue())
    stats = await survivor.run()
    # Joined although the queue was empty: a lease was outstanding.
    assert stats.resumed and stats.leases_reclaimed == 1
    assert len(hits) == PAGES + 1 and set(hits.values()) == {1}
    assert stats.dead_letters == 0
    assert await queue.in_flight() == 0 and await leased_entries(queue) == {}


async def test_request_that_keeps_expiring_goes_to_the_dlq(make_crawler, make_queue, tmp_path):
    queue = make_queue()
    await seed(queue, "http://shop.test/")
    crashed = make_queue(clock=past())
    for _ in range(3):  # three workers died holding it
        await crashed.receive(lease_ttl=60)
        assert await queue.reclaim() == 1
    assert await leased_entries(queue) == {"http://shop.test/": 3}

    hits = Counter()
    crawler = engine(
        make_crawler, tmp_path, "b", counting_handler(hits), make_queue(), max_deliveries=3
    )
    stats = await crawler.run()
    assert hits == {} and stats.dead_letters == 1
    [dead] = read_jsonl(crawler.settings.resolved("dlq"))
    assert dead["reason"] == "lease_expired_too_often"
    assert dead["request"]["url"] == "http://shop.test/"
    assert await queue.size() == 0 and await queue.in_flight() == 0
    assert await leased_entries(queue) == {}


async def test_long_request_keeps_its_lease(make_crawler, make_queue, tmp_path):
    await seed(make_queue(), "http://shop.test/p/0")
    hits = Counter()

    class One(LinkedSpider):
        def parse(self, response):
            yield {"page": 0}

    # 0.5 s fetch with a 0.2 s lease: only extensions keep the idle peer from reclaiming it.
    handler = counting_handler(hits, slow={"/p/0": 0.5})
    crawlers = [
        make_crawler(
            One(),
            handler,
            queue=make_queue(),
            resume=True,
            output_dir=tmp_path / name,
            sink_path=tmp_path / name / "items.jsonl",
            lease_ttl=0.2,
        )
        for name in ("a", "b")
    ]
    stats = await asyncio.gather(*(c.run() for c in crawlers))
    assert hits == {"/p/0": 1}
    assert sum(s.leases_reclaimed + s.leases_lost for s in stats) == 0


async def test_cancelled_crawl_releases_its_leases(make_crawler, make_queue, tmp_path):
    queue = make_queue()
    await seed(queue, "http://shop.test/p/1", "http://shop.test/p/2")
    blocked = asyncio.Event()

    async def handler(request):
        await blocked.wait()  # never set: both requests hang in flight
        return html("unreachable")

    crawler = engine(make_crawler, tmp_path, "a", handler, make_queue())
    task = asyncio.create_task(crawler.run())
    await eventually(lambda: _at_least(queue.in_flight(), 2))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Released at once (no waiting for expiry), and not counted as deliveries.
    assert await queue.in_flight() == 0 and await queue.size() == 2
    assert await leased_entries(queue) == {}
    hits = Counter()
    stats = await engine(make_crawler, tmp_path, "b", counting_handler(hits), make_queue()).run()
    assert stats.leases_reclaimed == 0 and hits["/p/1"] == hits["/p/2"] == 1


async def test_graceful_stop_requeues_a_deferred_retry_in_place_of_its_lease(
    make_crawler, make_queue, tmp_path
):
    queue = make_queue()
    await seed(queue, "http://shop.test/p/1")

    def handler(request):
        return html("busy", 503, {"Retry-After": "300"})

    crawler = engine(make_crawler, tmp_path, "a", handler, make_queue(), clock=SystemClock())
    task = asyncio.create_task(crawler.run())
    await eventually(lambda: _truthy(crawler._deferred))
    assert await queue.in_flight() == 1  # held while waiting for the retry
    crawler.stop()
    stats = await task
    assert stats.finish_reason == "stopped" and stats.retries == 1
    assert await queue.in_flight() == 0
    [pending] = (await queue.snapshot())["pending"]
    assert pending["url"] == "http://shop.test/p/1" and pending["retries"] == 1
