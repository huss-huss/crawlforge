import asyncio
import json
from collections import Counter

import fakeredis
import pytest

from crawlforge.checkpoint import Checkpoint
from crawlforge.http import Request
from crawlforge.queues.redis import RedisQueue
from test_engine import CatalogSpider, catalog_handler, html, read_jsonl


async def test_stop_and_resume_from_checkpoint_file(make_crawler, tmp_path):
    hits = Counter()
    ckpt = tmp_path / "crawl.checkpoint.json"
    first = make_crawler(
        CatalogSpider(), catalog_handler(hits), max_requests=2, concurrency=1, checkpoint_path=ckpt
    )
    stats1 = await first.run()
    assert stats1.finish_reason == "max_requests"
    saved = json.loads(ckpt.read_text())
    assert saved["spider"] == "catalog"
    assert saved["frontier"]["pending"] and saved["frontier"]["seen"]

    second = make_crawler(CatalogSpider(), catalog_handler(hits), resume=True, checkpoint_path=ckpt)
    stats2 = await second.run()
    assert stats2.resumed and stats2.finish_reason == "finished"
    # No page was fetched twice across the two runs, and nothing was missed.
    assert set(hits.values()) == {1} and len(hits) == 6
    names = sorted(i["name"] for i in read_jsonl(second.test_sink_path))
    assert names == ["One", "Three", "Two"]
    # Totals span both runs; the checkpoint is removed once the crawl completes.
    assert stats2.items == 3 and stats2.requests == 6
    assert not ckpt.exists()


async def test_cancelled_crawl_keeps_in_flight_requests(make_crawler, tmp_path):
    ckpt = tmp_path / "crash.checkpoint.json"
    release = asyncio.Event()
    started = asyncio.Event()
    hits = Counter()

    async def handler(request):
        hits[request.url.path] += 1
        if request.url.path == "/slow":
            started.set()
            await release.wait()
        return html("<h1>x</h1>")

    class S(CatalogSpider):
        name = "slow"
        start_urls = ["http://shop.test/slow"]

        def parse(self, response):
            yield {"url": response.url, "name": "slow", "price": 1}

    crawler = make_crawler(S(), handler, checkpoint_path=ckpt)
    task = asyncio.create_task(crawler.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    pending = json.loads(ckpt.read_text())["frontier"]["pending"]
    assert [p["url"] for p in pending] == ["http://shop.test/slow"]

    release.set()
    resumed = make_crawler(S(), handler, checkpoint_path=ckpt, resume=True)
    stats = await resumed.run()
    assert stats.items == 1 and stats.finish_reason == "finished"


async def test_periodic_snapshot_includes_active_and_deferred(make_crawler, tmp_path):
    ckpt = tmp_path / "p.checkpoint.json"
    crawler = make_crawler(CatalogSpider(), lambda r: html(""), checkpoint_path=ckpt)
    await crawler.frontier.push(Request("http://shop.test/queued"))
    crawler._active[1] = Request("http://shop.test/active")
    crawler._defer(Request("http://shop.test/deferred"), 3600)
    await crawler.save_checkpoint()
    urls = {p["url"] for p in json.loads(ckpt.read_text())["frontier"]["pending"]}
    assert urls == {
        "http://shop.test/queued",
        "http://shop.test/active",
        "http://shop.test/deferred",
    }
    for task in list(crawler._deferred):
        task.cancel()


def test_checkpoint_rejects_other_spider(tmp_path):
    ckpt = Checkpoint(tmp_path / "c.json")
    ckpt.save(spider="a", frontier={"pending": [], "seen": []}, stats={})
    assert ckpt.load(spider="a")["spider"] == "a"
    with pytest.raises(ValueError, match="belongs to spider"):
        ckpt.load(spider="b")


async def test_resume_from_persistent_redis_queue(make_crawler):
    server = fakeredis.FakeServer()
    hits = Counter()

    def queue():
        return RedisQueue(fakeredis.FakeAsyncRedis(server=server), prefix="crawlforge:catalog")

    first = make_crawler(
        CatalogSpider(), catalog_handler(hits), queue=queue(), max_requests=3, concurrency=1
    )
    await first.run()
    assert await queue().size() > 0

    second = make_crawler(CatalogSpider(), catalog_handler(hits), queue=queue(), resume=True)
    stats = await second.run()
    assert stats.resumed
    assert set(hits.values()) == {1} and len(hits) == 6


async def test_fresh_run_clears_stale_persistent_state(make_crawler):
    server = fakeredis.FakeServer()
    stale = RedisQueue(fakeredis.FakeAsyncRedis(server=server), prefix="crawlforge:catalog")
    await stale.add_seen(Request("http://shop.test/").fingerprint)
    await stale.push(Request("http://elsewhere.test/old"))

    hits = Counter()

    def handler(request):
        assert request.url.host == "shop.test"
        return catalog_handler(hits)(request)

    queue = RedisQueue(fakeredis.FakeAsyncRedis(server=server), prefix="crawlforge:catalog")
    stats = await make_crawler(CatalogSpider(), handler, queue=queue).run()
    assert stats.items == 3
