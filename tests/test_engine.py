import asyncio
import itertools
import json
from collections import Counter, defaultdict

import httpx
from pydantic import BaseModel, HttpUrl

from crawlforge import Request, Spider
from crawlforge.clock import FakeClock


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def html(body, status=200, headers=None):
    return httpx.Response(
        status, headers={"content-type": "text/html", **(headers or {})}, text=body
    )


class Product(BaseModel):
    url: HttpUrl
    name: str
    price: float


CATALOG = {
    "/": "<a href='/p/1'>1</a><a href='/p/2'>2</a><a href='/p/1#dup'>dup</a>"
    "<a href='/page/2'>next</a>",
    "/page/2": "<a href='/p/3'>3</a><a href='/p/bad'>bad</a><a href='/'>home</a>",
    "/p/1": "<h1>One</h1><span class='price'>1.50</span>",
    "/p/2": "<h1>Two</h1><span class='price'>2.00</span>",
    "/p/3": "<h1>Three</h1><span class='price'>3.25</span>",
    "/p/bad": "<h1>Bad</h1><span class='price'>call us</span>",
}


def catalog_handler(hits):
    def handler(request):
        hits[request.url.path] += 1
        body = CATALOG.get(request.url.path)
        return html(body) if body is not None else html("missing", 404)

    return handler


class CatalogSpider(Spider):
    name = "catalog"
    start_urls = ["http://shop.test/"]
    item_model = Product

    def parse(self, response):
        for href in response.css("a::attr(href)").getall():
            callback = self.parse_product if href.startswith("/p/") else self.parse
            yield response.follow(href, callback=callback)

    def parse_product(self, response):
        yield {
            "url": response.url,
            "name": response.css("h1::text").get(),
            "price": response.css(".price::text").get(),
        }


async def test_crawl_extracts_validates_and_dedupes(make_crawler):
    hits = Counter()
    crawler = make_crawler(CatalogSpider(), catalog_handler(hits))
    stats = await crawler.run()

    items = read_jsonl(crawler.test_sink_path)
    assert sorted(i["name"] for i in items) == ["One", "Three", "Two"]
    assert all(isinstance(i["price"], float) for i in items)
    # Every page fetched exactly once despite the duplicate links.
    assert set(hits.values()) == {1}
    assert len(hits) == 6
    assert stats.duplicates == 2  # "/p/1#dup" and the link back to "/"
    assert stats.items == 3 and stats.rejects == 1
    assert stats.finish_reason == "finished"

    rejects = read_jsonl(crawler.settings.resolved("rejects"))
    assert len(rejects) == 1
    assert rejects[0]["url"] == "http://shop.test/p/bad"
    assert rejects[0]["schema"] == "Product"
    assert rejects[0]["errors"][0]["loc"] == ["price"]
    assert json.loads(crawler.settings.resolved("stats").read_text())["items"] == 3


async def test_retries_transient_status_then_succeeds(make_crawler):
    calls = Counter()

    def handler(request):
        calls[request.url.path] += 1
        if calls[request.url.path] <= 2:
            return html("busy", 503)
        return html("<h1>ok</h1>")

    class S(Spider):
        name = "s"
        start_urls = ["http://flaky.test/x"]

        def parse(self, response):
            yield {"h1": response.css("h1::text").get()}

    crawler = make_crawler(S(), handler, max_retries=3)
    stats = await crawler.run()
    assert calls["/x"] == 3
    assert stats.retries == 2 and stats.items == 1 and stats.dead_letters == 0
    assert stats.status_counts == {"200": 1, "503": 2}


async def test_retry_after_header_is_honored(make_crawler):
    calls = Counter()

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return html("slow down", 429, {"Retry-After": "7"})
        return html("<h1>ok</h1>")

    class S(Spider):
        name = "s"
        start_urls = ["http://limited.test/"]

        def parse(self, response):
            yield {"ok": True}

    clock = FakeClock()
    crawler = make_crawler(S(), handler, clock=clock)
    stats = await crawler.run()
    assert stats.items == 1 and stats.retries == 1
    assert 7.0 in clock.sleeps


async def test_permanent_failures_go_to_dead_letter_queue(make_crawler):
    calls = Counter()

    def handler(request):
        calls[request.url.path] += 1
        if request.url.path == "/gone":
            return html("nope", 404)
        if request.url.path == "/down":
            return html("err", 500)
        raise httpx.ConnectError("connection refused", request=request)

    class S(Spider):
        name = "s"
        start_urls = ["http://a.test/gone", "http://a.test/down", "http://b.test/refused"]

        def parse(self, response):  # pragma: no cover - never reached
            yield {}

    crawler = make_crawler(S(), handler, max_retries=2, breaker_threshold=100)
    stats = await crawler.run()

    assert calls == {"/gone": 1, "/down": 3, "/refused": 3}
    dlq = {e["request"]["url"]: e for e in read_jsonl(crawler.settings.resolved("dlq"))}
    assert dlq["http://a.test/gone"]["reason"] == "http_404"
    assert dlq["http://a.test/gone"]["attempts"] == 1
    assert dlq["http://a.test/down"]["reason"] == "max_retries_exceeded:http_500"
    assert dlq["http://a.test/down"]["status"] == 500
    assert dlq["http://a.test/down"]["attempts"] == 3
    assert dlq["http://b.test/refused"]["reason"] == "max_retries_exceeded:ConnectError"
    assert stats.dead_letters == 3 and stats.items == 0
    assert stats.finish_reason == "finished"


async def test_handle_statuses_passes_errors_to_callback(make_crawler):
    class S(Spider):
        name = "s"
        start_urls = ["http://a.test/missing"]

        def parse(self, response):
            yield {"status": response.status}

    crawler = make_crawler(S(), lambda r: html("", 404), handle_statuses=[404])
    stats = await crawler.run()
    assert read_jsonl(crawler.test_sink_path) == [{"status": 404}]
    assert stats.dead_letters == 0


async def test_circuit_breaker_pauses_failing_domain_only(make_crawler):
    clock = FakeClock()
    bad_hits: list[float] = []

    def handler(request):
        if request.url.host == "bad.test":
            bad_hits.append(clock.monotonic())
            return html("down", 503)
        return html("<h1>fine</h1>")

    class S(Spider):
        name = "s"
        start_urls = [f"http://bad.test/{i}" for i in range(3)] + [
            f"http://good.test/{i}" for i in range(3)
        ]

        def parse(self, response):
            yield {"url": response.url}

    crawler = make_crawler(
        S(),
        handler,
        clock=clock,
        max_retries=4,
        breaker_threshold=2,
        breaker_recovery=60.0,
        backoff_base=0.001,
        backoff_cap=0.001,
    )
    stats = await crawler.run()

    assert stats.items == 3  # good.test unaffected
    assert stats.dead_letters == 3
    assert stats.breaker_trips >= 1
    # Once open, the domain was left alone for the full recovery window.
    gaps = [b - a for a, b in itertools.pairwise(bad_hits)]
    assert max(gaps) >= 60.0
    # The breaker delays attempts; every request still uses all 5 before dead-lettering.
    assert len(bad_hits) == 3 * 5


async def test_breaker_recovers_after_successful_probe(make_crawler):
    clock = FakeClock()
    state = {"healthy_after": 30.0}

    def handler(request):
        if clock.monotonic() < state["healthy_after"]:
            return html("down", 503)
        return html("<h1>up</h1>")

    class S(Spider):
        name = "s"
        start_urls = [f"http://svc.test/{i}" for i in range(4)]

        def parse(self, response):
            yield {"url": response.url}

    crawler = make_crawler(
        S(), handler, clock=clock, max_retries=10, breaker_threshold=2, breaker_recovery=20.0
    )
    stats = await crawler.run()
    assert stats.items == 4 and stats.dead_letters == 0
    assert crawler.breakers.get("svc.test").state.name == "CLOSED"


async def test_robots_txt_is_respected(make_crawler):
    hits = Counter()

    def handler(request):
        hits[request.url.path] += 1
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /private/\n")
        return html("<a href='/private/secret'>x</a><a href='/public'>y</a>")

    class S(Spider):
        name = "s"
        start_urls = ["http://polite.test/"]

        def parse(self, response):
            yield {"url": response.url}
            for href in response.css("a::attr(href)").getall():
                yield response.follow(href)

    crawler = make_crawler(S(), handler, obey_robots=True)
    stats = await crawler.run()
    assert hits["/private/secret"] == 0
    assert hits["/robots.txt"] == 1
    assert stats.robots_blocked == 1
    assert stats.items == 2


async def test_max_depth_limits_follow_chain(make_crawler):
    def handler(request):
        n = int(request.url.path.strip("/") or 0)
        return html(f"<a href='/{n + 1}'>next</a>")

    class S(Spider):
        name = "s"
        start_urls = ["http://deep.test/0"]

        def parse(self, response):
            yield {"depth": response.request.depth}
            yield response.follow(response.css("a::attr(href)").get())

    crawler = make_crawler(S(), handler, max_depth=2)
    stats = await crawler.run()
    assert sorted(i["depth"] for i in read_jsonl(crawler.test_sink_path)) == [0, 1, 2]
    assert stats.too_deep == 1


async def test_callback_errors_are_dead_lettered(make_crawler):
    class S(Spider):
        name = "s"
        start_urls = ["http://a.test/"]

        def parse(self, response):
            yield {"ok": 1}
            raise RuntimeError("selector broke")

    crawler = make_crawler(S(), lambda r: html("x"))
    stats = await crawler.run()
    assert stats.items == 1 and stats.callback_errors == 1
    (entry,) = read_jsonl(crawler.settings.resolved("dlq"))
    assert entry["reason"] == "callback_error:RuntimeError"
    assert "selector broke" in entry["error"]


async def test_async_callbacks_and_async_start_requests(make_crawler):
    class S(Spider):
        name = "s"

        async def start_requests(self):
            for i in range(3):
                yield Request(f"http://a.test/{i}", callback="parse_async", priority=i)

        async def parse_async(self, response):
            await asyncio.sleep(0)
            yield {"url": response.url}

    crawler = make_crawler(S(), lambda r: html("x"))
    stats = await crawler.run()
    assert stats.items == 3


async def test_per_domain_concurrency_is_capped(make_crawler):
    active = defaultdict(int)
    peak = defaultdict(int)

    async def handler(request):
        host = request.url.host
        active[host] += 1
        peak[host] = max(peak[host], active[host])
        await asyncio.sleep(0.01)
        active[host] -= 1
        return html("x")

    class S(Spider):
        name = "s"
        start_urls = [f"http://{h}.test/{i}" for h in ("a", "b") for i in range(10)]

        def parse(self, response):
            yield {"u": response.url}

    crawler = make_crawler(S(), handler, concurrency=8, per_domain_concurrency=2)
    stats = await crawler.run()
    assert stats.items == 20
    assert peak == {"a.test": 2, "b.test": 2}


async def test_max_requests_stops_gracefully(make_crawler):
    hits = Counter()
    crawler = make_crawler(CatalogSpider(), catalog_handler(hits), max_requests=2, concurrency=1)
    stats = await crawler.run()
    assert stats.requests == 2
    assert stats.finish_reason == "max_requests"
    assert await crawler.frontier.size() > 0
