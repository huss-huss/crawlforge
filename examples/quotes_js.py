"""Crawl the JavaScript-rendered https://quotes.toscrape.com/js/ through Playwright.

Requires the browser extra:

    pip install 'crawlforge[browser]' && playwright install chromium
    crawlforge run examples/quotes_js.py --sink jsonl:out/quotes_js.jsonl

Without Playwright the requests land in the dead-letter queue with an install hint.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from crawlforge import Request, Response, Spider


class Quote(BaseModel):
    text: str = Field(min_length=1)
    author: str = Field(min_length=1)
    tags: list[str]


def rendered(url: str) -> Request:
    # The quotes are injected by a <script>; wait until they exist in the DOM.
    return Request(url, render=True, meta={"wait_for": "div.quote"})


class QuotesJsSpider(Spider):
    name = "quotes_js"
    item_model = Quote
    custom_settings = {"rate_per_domain": 1.0, "per_domain_concurrency": 2}

    def start_requests(self):
        yield rendered("https://quotes.toscrape.com/js/")

    def parse(self, response: Response):
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get("").strip("“”"),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("a.tag::text").getall(),
            }
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield rendered(response.urljoin(next_page))
