import sys

import pytest

from crawlforge import Request, Response, Spider
from crawlforge.render import PlaywrightRenderer, RendererUnavailable
from test_engine import html, read_jsonl


class FakeRenderer:
    def __init__(self):
        self.urls = []
        self.closed = False

    async def fetch(self, request, timeout):
        self.urls.append(request.url)
        body = b"<div class='quote'>rendered by js</div>"
        return Response(request.url, 200, {}, body, request, rendered=True)

    async def close(self):
        self.closed = True


class MixedSpider(Spider):
    name = "mixed"

    def start_requests(self):
        yield Request("http://site.test/static")
        yield Request("http://site.test/js", render=True, meta={"wait_for": ".quote"})

    def parse(self, response):
        yield {
            "url": response.url,
            "rendered": response.rendered,
            "text": response.css(".quote::text").get(),
        }


async def test_render_is_selected_per_request(make_crawler):
    static_hits = []

    def handler(request):
        static_hits.append(str(request.url))
        return html("<div class='quote'>static</div>")

    crawler = make_crawler(MixedSpider(), handler)
    renderer = FakeRenderer()
    crawler.renderer = renderer
    await crawler.run()

    items = {i["url"]: i for i in read_jsonl(crawler.test_sink_path)}
    assert items["http://site.test/static"] == {
        "url": "http://site.test/static",
        "rendered": False,
        "text": "static",
    }
    assert items["http://site.test/js"]["rendered"] is True
    assert static_hits == ["http://site.test/static"]
    assert renderer.urls == ["http://site.test/js"] and renderer.closed


async def test_missing_playwright_dead_letters_render_requests(make_crawler, monkeypatch):
    monkeypatch.setitem(sys.modules, "playwright.async_api", None)
    crawler = make_crawler(MixedSpider(), lambda r: html("<div class='quote'>s</div>"))
    stats = await crawler.run()
    assert stats.items == 1
    (entry,) = read_jsonl(crawler.settings.resolved("dlq"))
    assert entry["reason"] == "fetch_error:RendererUnavailable"
    assert "crawlforge[browser]" in entry["error"]


async def test_renderer_raises_helpful_error_without_playwright(monkeypatch):
    monkeypatch.setitem(sys.modules, "playwright.async_api", None)
    with pytest.raises(RendererUnavailable, match="playwright install"):
        await PlaywrightRenderer("ua").fetch(Request("http://x.test/"), timeout=1)


async def test_real_browser_renders_javascript():
    """Runs only where Playwright and a Chromium build are installed (skipped in CI)."""
    pytest.importorskip("playwright.async_api")
    page = (
        "data:text/html,<div id=app></div>"
        "<script>document.getElementById('app').innerHTML='<p class=q>from js</p>'</script>"
    )
    renderer = PlaywrightRenderer("CrawlForgeTest/1.0")
    try:
        response = await renderer.fetch(Request(page, meta={"wait_for": "p.q"}), timeout=20)
    except RendererUnavailable as exc:
        pytest.skip(f"browser not available: {exc}")
    finally:
        await renderer.close()
    assert response.rendered
    assert response.css("p.q::text").get() == "from js"
