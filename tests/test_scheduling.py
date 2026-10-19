"""Per-domain scheduling: the next request comes from a domain that is ready (issue #3).

Queue-level tests run against the memory backend, fakeredis and, when ``TEST_REDIS_URL`` is
set, a real Redis. Engine tests use the fake clock, so the timings asserted here are exact.
"""

import heapq
import itertools
import json
import random
from collections import Counter

import httpx
import pytest

from crawlforge import Request, Spider
from crawlforge.clock import FakeClock
from crawlforge.frontier import Frontier
from crawlforge.queues import MemoryQueue, QueueBackend
from test_engine import html, read_jsonl


async def drain(queue, skip=()):
    out = []
    while (req := await queue.pop(skip)) is not None:
        out.append(req.url.removeprefix("http://"))
    return out


async def push_all(queue, *specs):
    for spec in specs:
        url, _, prio = spec.partition("@")
        await queue.push(Request(f"http://{url}", priority=int(prio or 0)))


# ---------------------------------------------------------------------- queue level


async def test_priority_within_a_domain_and_round_robin_across_domains(make_any_queue):
    queue = make_any_queue()
    await push_all(queue, "a.test/1", "a.test/2@5", "a.test/3", "b.test/1", "b.test/2", "c.test/1")
    assert await queue.size() == 6
    # a.test's best request outranks everything; after that the domains take turns, and
    # each domain serves its own requests by priority, then FIFO.
    assert await drain(queue) == [
        "a.test/2",
        "b.test/1",
        "c.test/1",
        "a.test/1",
        "b.test/2",
        "a.test/3",
    ]
    assert await queue.size() == 0


async def test_skipped_domain_keeps_its_requests_and_its_turn(make_any_queue):
    queue = make_any_queue()
    await push_all(queue, "a.test/1", "b.test/1", "b.test/2", "c.test/1", "a.test/2")
    assert (await queue.pop(skip={"a.test"})).url == "http://b.test/1"
    assert await queue.queued_domains(["a.test", "b.test", "z.test"]) == {"a.test", "b.test"}
    # a.test was passed over, not sent to the back: it's first once it is ready again.
    assert await drain(queue) == ["a.test/1", "c.test/1", "b.test/2", "a.test/2"]

    await push_all(queue, "a.test/3", "a.test/4", "b.test/3")
    assert await drain(queue, skip={"a.test"}) == ["b.test/3"]
    assert await queue.pop(skip={"a.test"}) is None
    assert await queue.size() == 2
    assert await drain(queue) == ["a.test/3", "a.test/4"]
    assert await queue.queued_domains(["a.test"]) == set()


async def test_snapshot_keeps_global_priority_order_across_domains(make_any_queue):
    queue = make_any_queue()
    frontier = Frontier(queue)
    for spec in ["b.test/1", "a.test/1@2", "c.test/1@-1", "a.test/2", "b.test/2@2"]:
        url, _, prio = spec.partition("@")
        await frontier.push(Request(f"http://{url}", priority=int(prio or 0)))
    snap = await queue.snapshot()
    expected = ["a.test/1", "b.test/2", "b.test/1", "a.test/2", "c.test/1"]
    assert [p["url"].removeprefix("http://") for p in snap["pending"]] == expected

    fresh = MemoryQueue()
    await fresh.restore(snap)
    assert await fresh.size() == 5 and await fresh.seen_count() == 5
    assert await drain(fresh) == ["a.test/1", "b.test/2", "a.test/2", "b.test/1", "c.test/1"]


async def test_leased_entries_return_to_their_own_domain(make_queue):
    now = [1000.0]
    queue = make_queue(clock=lambda: now[0])
    await push_all(queue, "a.test/1", "a.test/2", "b.test/1", "c.test/1")
    first = await queue.receive(30)  # a.test/1
    second = await queue.receive(30, skip={"a.test"})  # b.test/1
    third = await queue.receive(30)  # c.test/1
    assert [d.request.url for d in (first, second, third)] == [
        "http://a.test/1",
        "http://b.test/1",
        "http://c.test/1",
    ]
    second.request.retries = 1
    assert await queue.requeue(second.lease, second.request)
    assert await queue.release([third.lease]) == 1
    now[0] += 60
    assert await queue.reclaim() == 1  # first
    assert await queue.size() == 4 and await queue.in_flight() == 0
    # Every entry is back in its domain queue (and the index knows those domains).
    assert await queue.queued_domains(["a.test", "b.test", "c.test"]) == {
        "a.test",
        "b.test",
        "c.test",
    }
    got = []
    while (d := await queue.receive(30, skip={"c.test"})) is not None:
        got.append((d.request.url, d.request.retries, d.deliveries))
        assert await queue.ack(d.lease)
    assert sorted(got) == [
        ("http://a.test/1", 0, 2),  # reclaimed: its second delivery
        ("http://a.test/2", 0, 1),
        ("http://b.test/1", 1, 1),  # the retry
    ]
    assert [d.request.url for d in [await queue.receive(30)]] == ["http://c.test/1"]
    assert await queue.size() == 0
    assert await queue._redis.zcard(queue.domains_key) == 0


async def test_old_single_queue_frontier_is_migrated(make_queue):
    queue = make_queue()
    redis = queue._redis
    # The layout before per-domain queues: "<seq>|<json>" members in <prefix>:queue.
    old = {}
    for seq, (url, prio) in enumerate(
        [("http://a.test/1", 0), ("http://b.test/1", 3), ("http://a.test/2", 3)], start=1
    ):
        payload = json.dumps(Request(url, priority=prio).to_dict(), separators=(",", ":"))
        old[f"{seq}|{payload}"] = queue._score(prio, seq)
    await redis.zadd(queue.legacy_queue_key, old)
    await redis.set(queue.seq_key, 3)
    await redis.hset(queue.deliveries_key, next(iter(old)), 2)  # a.test/1 was reclaimed once

    fresh = make_queue()
    assert await fresh.size() == 3
    assert not await redis.exists(queue.legacy_queue_key)
    order = []
    while (d := await fresh.receive(30)) is not None:
        order.append((d.request.url, d.deliveries))
        await fresh.ack(d.lease)
    assert order == [("http://b.test/1", 1), ("http://a.test/2", 1), ("http://a.test/1", 3)]


# ---------------------------------------------------------------------- engine


class FifoQueue(QueueBackend):
    """The scheduler before per-domain queues: one global heap, ``skip`` ignored."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, Request]] = []
        self._seq = itertools.count()
        self._seen: set[str] = set()

    async def push(self, request):
        heapq.heappush(self._heap, (-request.priority, next(self._seq), request))

    async def pop(self, skip=()):
        return heapq.heappop(self._heap)[2] if self._heap else None

    async def queued_domains(self, domains):
        return set()

    async def size(self):
        return len(self._heap)

    async def add_seen(self, fp):
        if fp in self._seen:
            return False
        self._seen.add(fp)
        return True

    async def seen_count(self):
        return len(self._seen)

    async def snapshot(self):
        return {"pending": [r.to_dict() for *_, r in sorted(self._heap)], "seen": []}

    async def clear(self):
        self._heap.clear()
        self._seen.clear()


PER_SITE = 20


class TwoSites(Spider):
    """slow.test asks for a 1 s crawl-delay; fast.test has no limit. Slow URLs are seeded
    first, so a strict FIFO hands every worker a slow request before any fast one."""

    name = "two_sites"
    start_urls = [f"http://slow.test/{i}" for i in range(PER_SITE)] + [
        f"http://fast.test/{i}" for i in range(PER_SITE)
    ]

    def parse(self, response):
        yield {"url": response.url}


def two_sites_handler(clock, fetched):
    def handler(request):
        if request.url.path == "/robots.txt":
            delay = "Crawl-delay: 1\n" if request.url.host == "slow.test" else ""
            return httpx.Response(200, text=f"User-agent: *\nAllow: /\n{delay}")
        fetched.append((clock.monotonic(), request.url.host))
        return html("<p>ok</p>")

    return handler


async def crawl_two_sites(make_crawler, queue, tmp_path, name):
    clock = FakeClock()
    fetched: list[tuple[float, str]] = []
    crawler = make_crawler(
        TwoSites(),
        two_sites_handler(clock, fetched),
        clock=clock,
        queue=queue,
        obey_robots=True,
        burst=1.0,
        concurrency=4,
        per_domain_concurrency=4,
        output_dir=tmp_path / name,
        sink_path=tmp_path / name / "items.jsonl",
    )
    stats = await crawler.run()
    assert stats.items == 2 * PER_SITE and stats.finish_reason == "finished"
    assert len(read_jsonl(crawler.test_sink_path)) == 2 * PER_SITE
    by_host = {
        host: sorted(t for t, h in fetched if h == host) for host in ("slow.test", "fast.test")
    }
    return stats, by_host


async def test_throttled_domain_no_longer_blocks_the_others(make_crawler, make_any_queue, tmp_path):
    queue = make_any_queue()
    fifo_stats, fifo = await crawl_two_sites(make_crawler, FifoQueue(), tmp_path, "fifo")
    stats, ready = await crawl_two_sites(make_crawler, queue, tmp_path, "ready")

    # The slow domain is paced at its crawl-delay either way: 20 requests, 19 s apart.
    for times in (fifo["slow.test"], ready["slow.test"]):
        assert all(b - a >= 1.0 - 1e-6 for a, b in itertools.pairwise(times))
        assert times[-1] - times[0] == pytest.approx(PER_SITE - 1, abs=0.01)
    # FIFO: every worker waits in slow.test's token bucket; fast.test starts only once the
    # slow queue has drained into those waits.
    assert fifo["fast.test"][0] >= PER_SITE - 5
    # Per-domain scheduling: fast.test is done before slow.test's second token.
    fast_fifo, fast_ready = fifo["fast.test"][-1], ready["fast.test"][-1]
    assert fast_ready < 0.1
    fifo_rps = PER_SITE / fast_fifo
    ready_rps = PER_SITE / max(fast_ready, 1e-3)
    assert ready_rps >= 100 * fifo_rps, (fifo_rps, ready_rps)
    # And the crawl as a whole is no slower (it's bound by slow.test's crawl-delay).
    assert stats.elapsed_seconds <= fifo_stats.elapsed_seconds + 1e-6
    assert stats.elapsed_seconds == pytest.approx(PER_SITE - 1, abs=0.05)


class Recorder(Spider):
    name = "recorder"

    def __init__(self, specs):
        self.specs = specs

    def start_requests(self):
        for spec in self.specs:
            url, _, prio = spec.partition("@")
            yield Request(f"http://{url}", priority=int(prio or 0))

    def parse(self, response):
        yield {"url": response.url}


def recording_handler(order):
    def handler(request):
        order.append(f"{request.url.host}{request.url.path}")
        return html("<p>ok</p>")

    return handler


async def test_domains_take_turns(make_crawler, make_any_queue):
    specs = [f"{host}/{i}" for host in ("a.test", "b.test", "c.test") for i in range(4)]
    order: list[str] = []
    crawler = make_crawler(
        Recorder(specs), recording_handler(order), queue=make_any_queue(), concurrency=1
    )
    stats = await crawler.run()
    assert stats.items == 12
    assert [o.split("/")[0] for o in order] == ["a.test", "b.test", "c.test"] * 4
    assert [o for o in order if o.startswith("a.test")] == [f"a.test/{i}" for i in range(4)]


async def test_priority_order_within_a_domain(make_crawler, make_any_queue):
    specs = ["a.test/x", "b.test/1", "a.test/y@3", "a.test/z@1", "b.test/2", "a.test/w@3"]
    order: list[str] = []
    crawler = make_crawler(
        Recorder(specs), recording_handler(order), queue=make_any_queue(), concurrency=1
    )
    await crawler.run()
    assert [o for o in order if o.startswith("a.test")] == [
        "a.test/y",
        "a.test/w",
        "a.test/z",
        "a.test/x",
    ]
    # Priority still counts across domains: the high-priority requests go first.
    assert order[:3] == ["a.test/y", "a.test/w", "a.test/z"]
    assert sorted(order[3:]) == ["a.test/x", "b.test/1", "b.test/2"]


async def test_breaker_open_domain_is_skipped_until_half_open(make_crawler, make_any_queue):
    clock = FakeClock()
    hits: dict[str, list[float]] = {"bad.test": [], "good.test": []}

    def handler(request):
        hits[request.url.host].append(clock.monotonic())
        return html("down", 503) if request.url.host == "bad.test" else html("<p>ok</p>")

    specs = [f"bad.test/{i}" for i in range(6)] + [f"good.test/{i}" for i in range(6)]
    crawler = make_crawler(
        Recorder(specs),
        handler,
        clock=clock,
        queue=make_any_queue(),
        concurrency=2,
        max_retries=0,
        breaker_threshold=2,
        breaker_recovery=30.0,
    )
    deferred = Counter()
    original = crawler._defer

    def defer(request, delay):
        deferred[request.domain] += 1
        original(request, delay)

    crawler._defer = defer
    stats = await crawler.run()

    assert stats.items == 6 and stats.dead_letters == 6 and stats.breaker_trips >= 1
    assert hits["good.test"] == [0.0] * 6  # never held up by bad.test
    # Two failures open the breaker; after that one probe per recovery window, each failing.
    assert hits["bad.test"] == pytest.approx([0.0, 0.0, 30.0, 60.0, 90.0, 120.0])
    # Requests for the open domain stayed queued: none was popped only to be put back.
    assert deferred == {}


# ---------------------------------------------------------------------- resume


def multi_site_handler(hits):
    def handler(request):
        hits[f"{request.url.host}{request.url.path}"] += 1
        return html("<p>ok</p>")

    return handler


SITES = [f"{host}/{i}" for host in ("a.test", "b.test", "c.test") for i in range(5)]


async def test_checkpoint_resume_across_domains(make_crawler, tmp_path):
    hits = Counter()
    ckpt = tmp_path / "multi.ckpt.json"
    first = make_crawler(
        Recorder(SITES),
        multi_site_handler(hits),
        checkpoint_path=ckpt,
        max_requests=6,
        concurrency=1,
    )
    stats = await first.run()
    assert stats.finish_reason == "max_requests" and ckpt.exists()
    # Round-robin: the first six requests covered every domain twice.
    assert Counter(k.split("/")[0] for k in hits) == {"a.test": 2, "b.test": 2, "c.test": 2}

    second = make_crawler(
        Recorder(SITES), multi_site_handler(hits), checkpoint_path=ckpt, resume=True
    )
    stats = await second.run()
    assert stats.finish_reason == "finished" and stats.items == len(SITES)
    assert len(hits) == len(SITES) and set(hits.values()) == {1}
    assert not ckpt.exists()


async def test_resume_from_redis_across_domains(make_crawler, make_queue, tmp_path):
    hits = Counter()
    first = make_crawler(
        Recorder(SITES),
        multi_site_handler(hits),
        queue=make_queue(),
        max_requests=6,
        concurrency=1,
        output_dir=tmp_path / "a",
    )
    await first.run()
    queue = make_queue()
    assert await queue.size() == len(SITES) - 6 and await queue.in_flight() == 0
    second = make_crawler(
        Recorder(SITES),
        multi_site_handler(hits),
        queue=make_queue(),
        resume=True,
        output_dir=tmp_path / "b",
    )
    stats = await second.run()
    assert stats.resumed and stats.finish_reason == "finished"
    assert len(hits) == len(SITES) and set(hits.values()) == {1}
    queue = make_queue()
    assert await queue.size() == 0 and await queue._redis.zcard(queue.domains_key) == 0


async def test_redis_scripts_schedule_exactly_like_the_memory_queue(make_queue):
    """Random pushes, skips and pops: the Lua scheduler and MemoryQueue agree step by step."""
    rng = random.Random(3)
    memory, redis = MemoryQueue(), make_queue()
    domains = [f"d{i}.test" for i in range(5)]
    for step in range(400):
        if rng.random() < 0.55:
            request = Request(
                f"http://{rng.choice(domains)}/{step}", priority=rng.choice([0, 0, 0, 1, 5, -2])
            )
            await memory.push(request)
            await redis.push(request)
        else:
            skip = set(rng.sample(domains, rng.randint(0, 3)))
            got = await memory.pop(skip), await redis.pop(skip)
            assert got[0] == got[1], (step, skip)
    assert await memory.size() == await redis.size()
    assert await memory.snapshot() == await redis.snapshot()
    assert await drain(memory) == await drain(redis)
