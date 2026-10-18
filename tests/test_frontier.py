import fakeredis
import pytest

from crawlforge.frontier import Frontier, PushResult
from crawlforge.http import Request
from crawlforge.queues import MemoryQueue, queue_from_spec
from crawlforge.queues.redis import RedisQueue


async def drain(backend):
    out = []
    while (req := await backend.pop()) is not None:
        out.append(req)
    return out


async def test_priority_order_with_fifo_ties(backend):
    for name, prio in [("a", 0), ("b", 5), ("c", 0), ("d", 5), ("e", -1)]:
        await backend.push(Request(f"http://x.test/{name}", priority=prio))
    assert await backend.size() == 5
    order = [r.url.rsplit("/", 1)[1] for r in await drain(backend)]
    assert order == ["b", "d", "a", "c", "e"]
    assert await backend.pop() is None


async def test_requests_roundtrip_through_backend(backend):
    original = Request(
        "http://x.test/p", callback="parse_detail", meta={"a": [1, 2]}, depth=3, render=True
    )
    await backend.push(original)
    assert await backend.pop() == original


async def test_dedupe_uses_normalized_fingerprint(backend):
    frontier = Frontier(backend)
    assert await frontier.push(Request("http://X.test/a?b=1&a=2#top")) is PushResult.QUEUED
    assert await frontier.push(Request("http://x.test/a?a=2&b=1")) is PushResult.DUPLICATE
    assert await frontier.push(Request("http://x.test/a?a=2&b=1", method="POST")) is (
        PushResult.QUEUED
    )
    assert await frontier.push(Request("http://x.test/a", dont_filter=True)) is PushResult.QUEUED
    assert await frontier.push(Request("http://x.test/a", dont_filter=True)) is PushResult.QUEUED
    assert await frontier.size() == 4


async def test_max_depth_and_force(backend):
    frontier = Frontier(backend, max_depth=1)
    assert await frontier.push(Request("http://x.test/1", depth=1)) is PushResult.QUEUED
    assert await frontier.push(Request("http://x.test/2", depth=2)) is PushResult.TOO_DEEP
    seen = Request("http://x.test/1", depth=1)
    assert await frontier.push(seen) is PushResult.DUPLICATE
    assert await frontier.push(seen, force=True) is PushResult.QUEUED


async def test_snapshot_and_restore_preserve_order_and_seen(backend):
    frontier = Frontier(backend)
    for i, prio in enumerate([0, 3, 1]):
        await frontier.push(Request(f"http://x.test/{i}", priority=prio))
    snap = await backend.snapshot()
    assert len(snap["seen"]) == 3

    fresh = MemoryQueue()
    await fresh.restore(snap)
    assert [r.url for r in await drain(fresh)] == [
        "http://x.test/1",
        "http://x.test/2",
        "http://x.test/0",
    ]
    assert not await fresh.add_seen(Request("http://x.test/0").fingerprint)


async def test_redis_state_is_shared_between_instances():
    client = fakeredis.FakeAsyncRedis()
    first = RedisQueue(client, prefix="shared")
    await Frontier(first).push(Request("http://x.test/a"))
    second = RedisQueue(client, prefix="shared")
    assert second.persistent
    assert await second.size() == 1
    assert await Frontier(second).push(Request("http://x.test/a")) is PushResult.DUPLICATE
    other_prefix = RedisQueue(client, prefix="other")
    assert await other_prefix.size() == 0
    await second.clear()
    assert await first.size() == 0 and await first.seen_count() == 0


def test_queue_from_spec():
    assert isinstance(queue_from_spec("memory"), MemoryQueue)
    assert isinstance(queue_from_spec("redis://localhost:6379/0"), RedisQueue)
    with pytest.raises(ValueError):
        queue_from_spec("kafka://nope")
