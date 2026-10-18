"""The Spider API users subclass."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any, ClassVar

from pydantic import BaseModel

from crawlforge.http import Request, Response

#: What a callback may produce: items (dicts or pydantic models) and follow-up requests,
#: either returned as a single object/iterable or yielded from a (async) generator.
CallbackOutput = Request | BaseModel | dict[str, Any]


class Spider:
    """Declare where to start and how to parse.

    Example::

        class Books(Spider):
            name = "books"
            start_urls = ["https://books.toscrape.com/"]
            item_model = Book

            def parse(self, response):
                for href in response.css("h3 a::attr(href)").getall():
                    yield response.follow(href, callback=self.parse_book)

            def parse_book(self, response):
                yield {"title": response.css("h1::text").get(), ...}
    """

    name: ClassVar[str] = "spider"
    start_urls: ClassVar[list[str]] = []
    #: Schema used to validate dict items. Invalid items go to the rejects sink.
    item_model: ClassVar[type[BaseModel] | None] = None
    #: Per-spider settings overrides, e.g. ``{"rate_per_domain": 1.0}``.
    custom_settings: ClassVar[dict[str, Any]] = {}

    def start_requests(self) -> Iterable[Request] | AsyncIterator[Request]:
        for url in self.start_urls:
            yield Request(url, callback="parse")

    def parse(self, response: Response) -> Iterator[CallbackOutput] | Any:
        raise NotImplementedError(f"{type(self).__name__}.parse is not implemented")
