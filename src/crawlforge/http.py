"""Request and Response primitives."""

from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin

from parsel import Selector, SelectorList

from crawlforge.url import domain_of, fingerprint

Callback = str | Callable[..., Any] | None


def _callback_name(callback: Callback) -> str:
    if callback is None:
        return "parse"
    if isinstance(callback, str):
        return callback
    name = getattr(callback, "__name__", None)
    if not name:
        raise TypeError(f"callback must be a spider method or its name, got {callback!r}")
    return name


@dataclass(slots=True)
class Request:
    """A unit of work for the crawler.

    ``callback`` is stored by *name* so requests can be serialized into a
    checkpoint or a Redis queue and resolved against the spider later.
    """

    url: str
    callback: Callback = None
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    depth: int = 0
    render: bool = False
    dont_filter: bool = False
    retries: int = 0

    def __post_init__(self) -> None:
        self.callback = _callback_name(self.callback)
        self.method = self.method.upper()

    @property
    def domain(self) -> str:
        return domain_of(self.url)

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.method, self.url, self.body)

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "callback": self.callback,
            "method": self.method,
            "headers": dict(self.headers),
            "body": base64.b64encode(self.body).decode() if self.body else None,
            "meta": self.meta,
            "priority": self.priority,
            "depth": self.depth,
            "render": self.render,
            "dont_filter": self.dont_filter,
            "retries": self.retries,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Request:
        data = dict(data)
        body = data.pop("body", None)
        return cls(body=base64.b64decode(body) if body else None, **data)


@dataclass(slots=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: Request
    elapsed: float = 0.0
    rendered: bool = False
    _selector: Selector | None = field(default=None, repr=False)

    @property
    def text(self) -> str:
        charset = "utf-8"
        content_type = self.headers.get("content-type", "")
        if "charset=" in content_type:
            charset = content_type.split("charset=")[-1].split(";")[0].strip() or charset
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    @property
    def selector(self) -> Selector:
        if self._selector is None:
            self._selector = Selector(text=self.text, base_url=self.url)
        return self._selector

    def css(self, query: str) -> SelectorList:
        return self.selector.css(query)

    def xpath(self, query: str, **kwargs: Any) -> SelectorList:
        return self.selector.xpath(query, **kwargs)

    def json(self) -> Any:
        import json

        return json.loads(self.body)

    def urljoin(self, url: str) -> str:
        return urljoin(self.url, url)

    def follow(self, url: str, callback: Callback = None, **kwargs: Any) -> Request:
        """Build a child request relative to this response (depth handled by the engine)."""
        return Request(self.urljoin(url), callback=callback, **kwargs)
