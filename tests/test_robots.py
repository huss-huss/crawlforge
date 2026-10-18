import asyncio

import httpx

from crawlforge.clock import FakeClock
from crawlforge.fetch import HttpxFetcher
from crawlforge.robots import RobotsCache

UA = "CrawlForgeTest/1.0"

ROBOTS = """
User-agent: *
Disallow: /private/
Crawl-delay: 2

User-agent: CrawlForgeTest
Disallow: /no-forge/
Allow: /
"""


def make(handler, clock=None):
    fetcher = HttpxFetcher(UA, transport=httpx.MockTransport(handler))
    return RobotsCache(fetcher, UA, ttl=100, error_ttl=10, clock=clock or FakeClock())


async def test_rules_are_applied_for_matching_agent():
    def handler(request):
        assert request.url.path == "/robots.txt"
        return httpx.Response(200, text=ROBOTS)

    robots = make(handler)
    assert await robots.allowed("http://site.test/books/1")
    assert not await robots.allowed("http://site.test/no-forge/x")
    # The specific group wins over "*", so /private/ is allowed for our agent.
    assert await robots.allowed("http://site.test/private/x")


async def test_generic_rules_and_crawl_delay_for_other_agent():
    robots = make(lambda r: httpx.Response(200, text=ROBOTS))
    robots.user_agent = "SomethingElse/2.0"
    assert not await robots.allowed("http://site.test/private/x")
    assert await robots.crawl_delay("http://site.test/") == 2.0


async def test_cached_per_origin_with_single_fetch_under_concurrency():
    hits = []

    def handler(request):
        hits.append(str(request.url))
        return httpx.Response(200, text="User-agent: *\nDisallow:\n")

    robots = make(handler)
    await asyncio.gather(*(robots.allowed(f"http://a.test/{i}") for i in range(20)))
    await robots.allowed("http://b.test/")
    await robots.allowed("https://a.test/")
    assert sorted(hits) == [
        "http://a.test/robots.txt",
        "http://b.test/robots.txt",
        "https://a.test/robots.txt",
    ]


async def test_missing_robots_allows_everything():
    robots = make(lambda r: httpx.Response(404))
    assert await robots.allowed("http://site.test/anything")


async def test_server_error_disallows_and_rechecks_sooner():
    clock = FakeClock()
    responses = iter([httpx.Response(503), httpx.Response(200, text="User-agent: *\nDisallow:")])
    robots = make(lambda r: next(responses), clock=clock)
    assert not await robots.allowed("http://site.test/")
    clock.advance(5)
    assert not await robots.allowed("http://site.test/")
    clock.advance(6)  # past error_ttl
    assert await robots.allowed("http://site.test/")
    assert robots.fetch_count == 2


async def test_network_error_disallows():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    robots = make(handler)
    assert not await robots.allowed("http://down.test/")
