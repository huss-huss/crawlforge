"""Crawl https://quotes.toscrape.com (a sandbox built for scraping practice).

crawlforge run examples/quotes.py --sink jsonl:out/quotes.jsonl --sink csv:out/quotes.csv
"""

from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl

from crawlforge import Response, Spider


class Quote(BaseModel):
    text: str = Field(min_length=1)
    author: str = Field(min_length=1)
    author_url: HttpUrl
    tags: list[str]
    page: HttpUrl


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]
    item_model = Quote

    def parse(self, response: Response):
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get("").strip("“”"),
                "author": quote.css("small.author::text").get(),
                "author_url": response.urljoin(quote.css("span a::attr(href)").get("")),
                "tags": quote.css("a.tag::text").getall(),
                "page": response.url,
            }
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page)
