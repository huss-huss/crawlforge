"""Crawl the full catalogue of https://books.toscrape.com (a sandbox built for scraping practice).

crawlforge run examples/books.py --sink jsonl:out/books.jsonl --concurrency 8
"""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, HttpUrl, field_validator

from crawlforge import Response, Spider

RATINGS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


class Book(BaseModel):
    url: HttpUrl
    title: str = Field(min_length=1)
    price: float = Field(ge=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    in_stock: int = Field(ge=0)
    rating: int = Field(ge=1, le=5)
    upc: str = Field(pattern=r"^[0-9a-f]{16}$")
    category: str
    description: str | None = None

    @field_validator("title", "category")
    @classmethod
    def strip(cls, value: str) -> str:
        return value.strip()


class BooksSpider(Spider):
    name = "books"
    start_urls = ["https://books.toscrape.com/catalogue/page-1.html"]
    item_model = Book
    custom_settings = {"rate_per_domain": 5.0, "per_domain_concurrency": 4, "burst": 4}

    def parse(self, response: Response):
        # Pagination outranks detail pages: listing pages form a serial chain (each one
        # reveals the next), so fetching them first keeps the frontier full and lets the
        # detail pages saturate the per-domain rate limit instead of stalling behind it.
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse, priority=10)
        for href in response.css("article.product_pod h3 a::attr(href)").getall():
            yield response.follow(href, callback=self.parse_book)

    def parse_book(self, response: Response):
        table = {
            row.css("th::text").get(): row.css("td::text").get()
            for row in response.css("table.table-striped tr")
        }
        price_text = response.css("p.price_color::text").get("")
        stock_text = " ".join(response.css("p.availability::text").getall())
        rating_class = response.css("p.star-rating::attr(class)").get("")
        yield {
            "url": response.url,
            "title": response.css("div.product_main h1::text").get(),
            "price": re.sub(r"[^\d.]", "", price_text) or None,
            "currency": "GBP" if "£" in price_text else None,
            "in_stock": (re.search(r"\((\d+) available\)", stock_text) or [None, 0])[1],
            "rating": RATINGS.get(rating_class.replace("star-rating", "").strip()),
            "upc": table.get("UPC"),
            "category": response.css("ul.breadcrumb li:nth-child(3) a::text").get(),
            "description": response.css("#product_description + p::text").get(),
        }
