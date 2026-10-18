"""Fetchers turn a Request into a Response. The HTTP one is backed by httpx."""

from __future__ import annotations

import time
from typing import Protocol

import httpx

from crawlforge.http import Request, Response


class Fetcher(Protocol):
    async def fetch(self, request: Request, timeout: float) -> Response: ...

    async def close(self) -> None: ...


class HttpxFetcher:
    """Plain HTTP(S) fetcher. Pass ``transport`` to plug in ``httpx.MockTransport``."""

    def __init__(
        self,
        user_agent: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        follow_redirects: bool = True,
        max_connections: int = 100,
        http2: bool = False,
    ) -> None:
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent},
            transport=transport,
            follow_redirects=follow_redirects,
            limits=httpx.Limits(max_connections=max_connections),
            http2=http2,
        )

    async def fetch(self, request: Request, timeout: float) -> Response:
        started = time.perf_counter()
        resp = await self._client.request(
            request.method,
            request.url,
            headers=request.headers or None,
            content=request.body,
            timeout=timeout,
        )
        return Response(
            url=str(resp.url),
            status=resp.status_code,
            headers={k.lower(): v for k, v in resp.headers.items()},
            body=resp.content,
            request=request,
            elapsed=time.perf_counter() - started,
        )

    async def close(self) -> None:
        await self._client.aclose()
