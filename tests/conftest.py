import fakeredis
import pytest

from crawlforge.queues import MemoryQueue
from crawlforge.queues.redis import RedisQueue


@pytest.fixture(params=["memory", "redis"])
async def backend(request):
    if request.param == "memory":
        yield MemoryQueue()
        return
    client = fakeredis.FakeAsyncRedis()
    queue = RedisQueue(client, prefix="test")
    yield queue
    await queue.close()


import httpx  # noqa: E402

from crawlforge.clock import FakeClock  # noqa: E402
from crawlforge.engine import Crawler  # noqa: E402
from crawlforge.fetch import HttpxFetcher  # noqa: E402
from crawlforge.settings import Settings  # noqa: E402
from crawlforge.sinks import JsonlSink  # noqa: E402

FAST = {
    "obey_robots": False,
    "rate_per_domain": 1000.0,
    "burst": 1000.0,
    "concurrency": 4,
    "per_domain_concurrency": 4,
    "backoff_base": 0.01,
    "backoff_cap": 0.05,
    "breaker_recovery": 0.05,
    "timeout": 5.0,
}


@pytest.fixture
def make_crawler(tmp_path):
    """Build a Crawler wired to an httpx.MockTransport ``handler`` (no network)."""

    def factory(spider, handler, *, clock=None, queue=None, sink_path=None, **overrides):
        settings = Settings.build(FAST, {"output_dir": tmp_path / "state"}, overrides)
        fetcher = HttpxFetcher(settings.user_agent, transport=httpx.MockTransport(handler))
        sink = JsonlSink(sink_path or tmp_path / "items.jsonl", append=settings.resume)
        crawler = Crawler(
            spider,
            settings,
            sinks=[sink],
            fetcher=fetcher,
            clock=clock or FakeClock(),
            queue=queue,
        )
        crawler.test_sink_path = sink.path
        return crawler

    return factory
