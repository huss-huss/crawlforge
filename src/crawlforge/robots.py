"""robots.txt support, cached per origin (scheme + host + port)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

from crawlforge.clock import Clock, SystemClock
from crawlforge.fetch import Fetcher
from crawlforge.http import Request

log = logging.getLogger("crawlforge.robots")


@dataclass
class _Entry:
    parser: RobotFileParser
    fetched_at: float
    ttl: float


def _origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme.lower()}://{parts.netloc.lower()}"


class RobotsCache:
    """Fetches and caches robots.txt, following RFC 9309 failure semantics.

    - 2xx: rules are parsed and applied.
    - 4xx (including 404): no restrictions.
    - 5xx or network failure: assume complete disallow, and re-check after
      ``error_ttl`` seconds rather than the normal ``ttl``.
    """

    def __init__(
        self,
        fetcher: Fetcher,
        user_agent: str,
        *,
        ttl: float = 3600.0,
        error_ttl: float = 60.0,
        timeout: float = 10.0,
        clock: Clock | None = None,
    ) -> None:
        self._fetcher = fetcher
        self.user_agent = user_agent
        self.ttl = ttl
        self.error_ttl = error_ttl
        self.timeout = timeout
        self._clock = clock or SystemClock()
        self._entries: dict[str, _Entry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.fetch_count = 0

    async def _load(self, origin: str) -> RobotFileParser:
        robots_url = f"{origin}/robots.txt"
        parser = RobotFileParser(robots_url)
        ttl = self.ttl
        self.fetch_count += 1
        try:
            resp = await self._fetcher.fetch(Request(robots_url), timeout=self.timeout)
        except Exception as exc:
            log.warning("robots.txt fetch failed", extra={"url": robots_url, "error": repr(exc)})
            parser.disallow_all = True
            ttl = self.error_ttl
        else:
            if 200 <= resp.status < 300:
                parser.parse(resp.text.splitlines())
            elif 400 <= resp.status < 500:
                parser.allow_all = True
            else:
                parser.disallow_all = True
                ttl = self.error_ttl
        # RobotFileParser.can_fetch refuses everything until last_checked is set.
        parser.modified()
        self._entries[origin] = _Entry(parser, self._clock.monotonic(), ttl)
        return parser

    async def parser_for(self, url: str) -> RobotFileParser:
        origin = _origin(url)
        entry = self._entries.get(origin)
        if entry and self._clock.monotonic() - entry.fetched_at < entry.ttl:
            return entry.parser
        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            entry = self._entries.get(origin)
            if entry and self._clock.monotonic() - entry.fetched_at < entry.ttl:
                return entry.parser
            return await self._load(origin)

    async def allowed(self, url: str) -> bool:
        parser = await self.parser_for(url)
        return parser.can_fetch(self.user_agent, url)

    async def crawl_delay(self, url: str) -> float | None:
        parser = await self.parser_for(url)
        delay = parser.crawl_delay(self.user_agent)
        return float(delay) if delay is not None else None
