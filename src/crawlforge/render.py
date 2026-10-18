"""Optional Playwright renderer for JavaScript-heavy pages.

Playwright is imported lazily, so the core works without the ``[browser]``
extra. Requests opt in individually with ``Request(url, render=True)``.

Per-request ``meta`` options:

- ``wait_until``: Playwright load state (default ``"load"``)
- ``wait_for``: CSS selector that must appear before the HTML is captured
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from crawlforge.http import Request, Response

INSTALL_HINT = (
    "Playwright is not installed. Install the browser extra and a browser:\n"
    "  pip install 'crawlforge[browser]' && playwright install chromium"
)


class RendererUnavailable(RuntimeError):
    """Raised when a request needs rendering but Playwright is not usable."""


class PlaywrightRenderer:
    def __init__(
        self,
        user_agent: str,
        *,
        concurrency: int = 2,
        headless: bool = True,
        browser: str = "chromium",
        wait_until: str = "load",
    ) -> None:
        self.user_agent = user_agent
        self.headless = headless
        self.browser_name = browser
        self.wait_until = wait_until
        self._semaphore = asyncio.Semaphore(concurrency)
        self._start_lock = asyncio.Lock()
        self._playwright: Any = None
        self._browser: Any = None
        self._errors: tuple[type[BaseException], type[BaseException]] | None = None

    async def _ensure_started(self) -> None:
        if self._browser is not None:
            return
        async with self._start_lock:
            if self._browser is not None:
                return
            try:
                from playwright.async_api import Error as PlaywrightError
                from playwright.async_api import TimeoutError as PlaywrightTimeout
                from playwright.async_api import async_playwright
            except ImportError as exc:
                raise RendererUnavailable(INSTALL_HINT) from exc
            self._errors = (PlaywrightTimeout, PlaywrightError)
            self._playwright = await async_playwright().start()
            try:
                launcher = getattr(self._playwright, self.browser_name)
                self._browser = await launcher.launch(headless=self.headless)
            except PlaywrightError as exc:
                await self._playwright.stop()
                self._playwright = None
                raise RendererUnavailable(f"could not launch {self.browser_name}: {exc}") from exc

    async def fetch(self, request: Request, timeout: float) -> Response:
        await self._ensure_started()
        assert self._errors is not None
        playwright_timeout, playwright_error = self._errors
        started = time.perf_counter()
        async with self._semaphore:
            context = await self._browser.new_context(
                user_agent=self.user_agent, extra_http_headers=request.headers or None
            )
            try:
                page = await context.new_page()
                try:
                    resp = await page.goto(
                        request.url,
                        wait_until=request.meta.get("wait_until", self.wait_until),
                        timeout=timeout * 1000,
                    )
                    if selector := request.meta.get("wait_for"):
                        await page.wait_for_selector(selector, timeout=timeout * 1000)
                    html = await page.content()
                except playwright_timeout as exc:
                    raise TimeoutError(f"render timed out: {exc}") from exc
                except playwright_error as exc:
                    # Navigation failures (DNS, refused, reset) are worth retrying.
                    if "net::" in str(exc):
                        raise ConnectionError(str(exc)) from exc
                    raise
                headers = await resp.all_headers() if resp is not None else {}
                return Response(
                    url=page.url,
                    status=resp.status if resp is not None else 200,
                    headers={k.lower(): v for k, v in headers.items()},
                    body=html.encode("utf-8"),
                    request=request,
                    elapsed=time.perf_counter() - started,
                    rendered=True,
                )
            finally:
                await context.close()

    async def close(self) -> None:
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
