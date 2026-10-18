"""The crawl engine: workers pulling from the frontier through politeness and resilience layers.

Request lifecycle::

    frontier.pop -> robots check -> circuit breaker -> per-domain slot (semaphore + token
    bucket) -> fetch (HTTP or browser) -> classify -> retry / dead-letter / callback ->
    items (validate -> sinks | rejects) and follow-up requests (-> frontier)

Retries and requests refused by an open breaker are *deferred*: a timer task re-queues
them later, so a worker is never parked on a backoff sleep.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from crawlforge.breaker import BreakerRegistry, BreakerState, CircuitBreaker
from crawlforge.clock import Clock, SystemClock
from crawlforge.deadletter import DeadLetterQueue, RejectsSink
from crawlforge.fetch import Fetcher, HttpxFetcher
from crawlforge.frontier import Frontier, PushResult
from crawlforge.http import Request, Response
from crawlforge.metrics import Metrics
from crawlforge.queues import MemoryQueue, QueueBackend
from crawlforge.ratelimit import DomainThrottle
from crawlforge.retry import RetryPolicy, is_transient_exception, parse_retry_after
from crawlforge.robots import RobotsCache
from crawlforge.settings import Settings
from crawlforge.sinks.base import Sink
from crawlforge.sinks.files import JsonlSink
from crawlforge.spider import Spider
from crawlforge.stats import CrawlStats

log = logging.getLogger("crawlforge.engine")

_IDLE_POLL = 0.05


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Crawler:
    def __init__(
        self,
        spider: Spider,
        settings: Settings | None = None,
        *,
        sinks: Sequence[Sink] = (),
        queue: QueueBackend | None = None,
        fetcher: Fetcher | None = None,
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.spider = spider
        self.settings = settings or Settings.build(spider.custom_settings)
        s = self.settings
        self.clock = clock or SystemClock()
        self.sinks = list(sinks)
        self.frontier = Frontier(queue or MemoryQueue(), max_depth=s.max_depth)
        self.fetcher: Fetcher = fetcher or HttpxFetcher(
            s.user_agent, follow_redirects=s.follow_redirects
        )
        self.robots = (
            RobotsCache(self.fetcher, s.user_agent, timeout=s.timeout, clock=self.clock)
            if s.obey_robots
            else None
        )
        self.throttle = DomainThrottle(
            rate=s.rate_per_domain,
            burst=s.burst,
            concurrency=s.per_domain_concurrency,
            clock=self.clock,
        )
        self.breakers = BreakerRegistry(s.breaker_threshold, s.breaker_recovery, self.clock)
        self.retry = retry_policy or RetryPolicy(
            max_retries=s.max_retries,
            backoff_base=s.backoff_base,
            backoff_cap=s.backoff_cap,
            max_retry_after=s.max_retry_after,
        )
        self.metrics = metrics or Metrics()
        self.stats = CrawlStats(spider=spider.name)
        append = s.resume
        self.dlq = DeadLetterQueue(JsonlSink(s.resolved("dlq"), append=append))
        self.rejects = RejectsSink(JsonlSink(s.resolved("rejects"), append=append))

        self._active: dict[int, Request] = {}
        self._deferred: dict[asyncio.Task[None], Request] = {}
        self._crawl_delay_checked: set[str] = set()
        self._dispatched = 0
        self._idle = 0
        self._stopping = False
        self._stop_reason: str | None = None
        self._done = asyncio.Event()
        self._wakeup = asyncio.Event()

    # ------------------------------------------------------------------ control

    def stop(self, reason: str = "stopped") -> None:
        """Graceful stop: finish in-flight requests, keep the rest for a resume."""
        if not self._stopping:
            log.info("stopping crawl", extra={"reason": reason})
            self._stopping = True
            self._stop_reason = reason
            self._wakeup.set()

    async def run(self) -> CrawlStats:
        s = self.settings
        started = self.clock.monotonic()
        self.stats.started_at = _utcnow()
        for sink in (*self.sinks, self.dlq.sink, self.rejects.sink):
            await sink.open()
        if s.metrics_port is not None:
            self.metrics.serve(s.metrics_port, s.metrics_addr)
            log.info("metrics endpoint up", extra={"port": s.metrics_port})

        background: list[asyncio.Task[None]] = []
        error: BaseException | None = None
        try:
            await self._seed()
            background.append(asyncio.create_task(self._monitor()))
            workers = [asyncio.create_task(self._worker()) for _ in range(s.concurrency)]
            try:
                await asyncio.gather(*workers)
            except BaseException:
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                raise
        except BaseException as exc:
            error = exc
            raise
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._shutdown(error)
            self.stats.elapsed_seconds = round(self.clock.monotonic() - started, 3)
            self.stats.finished_at = _utcnow()
            self.stats.write(s.resolved("stats"))
            log.info("crawl finished", extra=self.stats.to_dict())
        return self.stats

    # ------------------------------------------------------------------ setup / teardown

    async def _seed(self) -> None:
        start = self.spider.start_requests()
        if hasattr(start, "__aiter__"):
            async for request in start:
                await self._schedule(request)
        else:
            for request in start:
                await self._schedule(request)

    async def _shutdown(self, error: BaseException | None) -> None:
        self._stopping = True
        # Anything not finished goes back to the frontier so a checkpoint keeps it.
        leftovers = list(self._deferred.items())
        for task, _ in leftovers:
            task.cancel()
        await asyncio.gather(*(t for t, _ in leftovers), return_exceptions=True)
        for request in [*self._active.values(), *(r for _, r in leftovers)]:
            await self.frontier.push(request, force=True)
        self._active.clear()
        self._deferred.clear()

        remaining = await self.frontier.size()
        if error is not None:
            self.stats.finish_reason = f"error: {type(error).__name__}"
        elif self._stop_reason:
            self.stats.finish_reason = self._stop_reason
        else:
            self.stats.finish_reason = "finished" if remaining == 0 else "stopped"

        for sink in self.sinks:
            try:
                await sink.close()
            except Exception:
                log.exception("sink close failed", extra={"sink": sink.name})
                self.stats.sink_errors += 1
        await self.dlq.sink.close()
        await self.rejects.sink.close()
        await self.fetcher.close()

    # ------------------------------------------------------------------ scheduling

    async def _schedule(self, request: Request, *, force: bool = False) -> PushResult:
        result = await self.frontier.push(request, force=force)
        if result is PushResult.DUPLICATE:
            self.stats.duplicates += 1
            self.metrics.dropped.labels("duplicate").inc()
        elif result is PushResult.TOO_DEEP:
            self.stats.too_deep += 1
            self.metrics.dropped.labels("too_deep").inc()
        else:
            self._wakeup.set()
        return result

    def _defer(self, request: Request, delay: float) -> None:
        async def requeue() -> None:
            await self.clock.sleep(delay)
            await self.frontier.push(request, force=True)
            self._wakeup.set()

        task = asyncio.create_task(requeue())
        self._deferred[task] = request
        task.add_done_callback(lambda t: self._deferred.pop(t, None))

    async def _worker(self) -> None:
        s = self.settings
        while not self._stopping and not self._done.is_set():
            if s.max_requests is not None and self._dispatched >= s.max_requests:
                self.stop("max_requests")
                return
            request = await self.frontier.pop()
            if request is None:
                if await self._wait_for_work():
                    return
                continue
            # No await between pop and registration: the request is never invisible.
            self._dispatched += 1
            key = id(request)
            self._active[key] = request
            try:
                await self._process(request)
            except asyncio.CancelledError:
                # Leave it in _active: _shutdown re-queues it for the checkpoint.
                raise
            except Exception as exc:
                log.exception("internal error processing request", extra={"url": request.url})
                await self._dead_letter(request, "internal_error", error=repr(exc))
            self._active.pop(key, None)

    async def _wait_for_work(self) -> bool:
        """Idle until new work may exist. Returns True when the crawl is complete."""
        self._idle += 1
        try:
            if (
                self._idle == self.settings.concurrency
                and not self._active
                and not self._deferred
                and await self.frontier.size() == 0
            ):
                self._done.set()
                self._wakeup.set()
                return True
            self._wakeup.clear()
            try:
                async with asyncio.timeout(_IDLE_POLL):
                    await self._wakeup.wait()
            except TimeoutError:
                pass
            return self._done.is_set()
        finally:
            self._idle -= 1

    # ------------------------------------------------------------------ request pipeline

    async def _process(self, request: Request) -> None:
        domain = request.domain
        if self.robots is not None and not request.meta.get("skip_robots"):
            if not await self.robots.allowed(request.url):
                self.stats.robots_blocked += 1
                self.metrics.dropped.labels("robots").inc()
                log.info("blocked by robots.txt", extra={"url": request.url})
                return
            if domain not in self._crawl_delay_checked:
                self._crawl_delay_checked.add(domain)
                delay = await self.robots.crawl_delay(request.url)
                if delay:
                    self.throttle.apply_crawl_delay(domain, delay)
                    log.info(
                        "applying robots crawl-delay", extra={"domain": domain, "delay": delay}
                    )

        breaker = self.breakers.get(domain)
        if not breaker.allow():
            self._defer(request, max(breaker.retry_in(), _IDLE_POLL))
            return

        try:
            async with self.throttle.slot(domain):
                self.stats.requests += 1
                self.metrics.requests.labels(domain).inc()
                response = await self._fetch(request)
        except Exception as exc:
            await self._on_fetch_error(request, breaker, exc)
            return

        self.stats.responses += 1
        self.stats.status_counts[str(response.status)] += 1
        self.metrics.responses.labels(domain, str(response.status)).inc()
        self.metrics.latency.labels(domain).observe(response.elapsed)
        log.debug(
            "fetched",
            extra={"url": request.url, "status": response.status, "elapsed": response.elapsed},
        )

        if self.retry.should_retry_status(response.status):
            self._breaker_failure(domain, breaker)
            retry_after = parse_retry_after(response.headers.get("retry-after"), self.clock.time())
            await self._retry_or_dead_letter(
                request, f"http_{response.status}", status=response.status, retry_after=retry_after
            )
            return

        self._breaker_success(domain, breaker)
        if response.status >= 400 and response.status not in self.settings.handle_statuses:
            await self._dead_letter(request, f"http_{response.status}", status=response.status)
            return
        await self._run_callback(response)

    async def _fetch(self, request: Request) -> Response:
        async with asyncio.timeout(self.settings.timeout):
            return await self.fetcher.fetch(request, timeout=self.settings.timeout)

    async def _on_fetch_error(
        self, request: Request, breaker: CircuitBreaker, exc: Exception
    ) -> None:
        domain = request.domain
        self.stats.fetch_errors += 1
        name = type(exc).__name__
        self.metrics.fetch_errors.labels(domain, name).inc()
        if is_transient_exception(exc):
            self._breaker_failure(domain, breaker)
            log.warning("transient fetch error", extra={"url": request.url, "error": name})
            await self._retry_or_dead_letter(request, name, error=repr(exc))
        else:
            # Not the server's fault (bad URL, unsupported scheme, ...): don't judge the domain.
            breaker.release_probe()
            log.error("fetch failed", extra={"url": request.url, "error": repr(exc)})
            await self._dead_letter(request, f"fetch_error:{name}", error=repr(exc))

    async def _retry_or_dead_letter(
        self,
        request: Request,
        reason: str,
        *,
        status: int | None = None,
        error: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        if not self.retry.can_retry(request.retries):
            await self._dead_letter(
                request, f"max_retries_exceeded:{reason}", status=status, error=error
            )
            return
        request.retries += 1
        delay = self.retry.delay(request.retries, retry_after)
        self.stats.retries += 1
        self.metrics.retries.labels(request.domain, reason).inc()
        log.info(
            "retry scheduled",
            extra={
                "url": request.url,
                "reason": reason,
                "attempt": request.retries,
                "delay": round(delay, 3),
                "retry_after": retry_after,
            },
        )
        self._defer(request, delay)

    async def _dead_letter(
        self, request: Request, reason: str, *, status: int | None = None, error: str | None = None
    ) -> None:
        self.stats.dead_letters += 1
        self.metrics.dead_letters.labels(request.domain, reason.split(":")[0]).inc()
        log.warning("dead-lettered", extra={"url": request.url, "reason": reason})
        await self.dlq.put(request, reason, status=status, error=error)

    def _breaker_failure(self, domain: str, breaker: CircuitBreaker) -> None:
        before = breaker.state
        breaker.record_failure()
        after = breaker.state
        if after is BreakerState.OPEN and before is not BreakerState.OPEN:
            self.stats.breaker_trips += 1
            self.metrics.breaker_trips.labels(domain).inc()
            log.warning(
                "circuit opened",
                extra={"domain": domain, "failures": breaker.consecutive_failures},
            )
        self.metrics.breaker_state.labels(domain).set(int(after))

    def _breaker_success(self, domain: str, breaker: CircuitBreaker) -> None:
        if breaker.state is not BreakerState.CLOSED:
            log.info("circuit closed", extra={"domain": domain})
        breaker.record_success()
        self.metrics.breaker_state.labels(domain).set(int(BreakerState.CLOSED))

    # ------------------------------------------------------------------ callbacks & items

    async def _run_callback(self, response: Response) -> None:
        request = response.request
        callback = getattr(self.spider, str(request.callback), None)
        if not callable(callback):
            await self._dead_letter(request, f"unknown_callback:{request.callback}")
            return
        try:
            result = callback(response)
            if inspect.isawaitable(result):
                result = await result
            if result is None:
                return
            if hasattr(result, "__aiter__"):
                async for output in result:
                    await self._handle_output(output, response)
            elif isinstance(result, Request | BaseModel | dict):
                await self._handle_output(result, response)
            elif isinstance(result, Iterable):
                for output in result:
                    await self._handle_output(output, response)
            else:
                raise TypeError(f"unsupported callback return type {type(result).__name__}")
        except Exception as exc:
            self.stats.callback_errors += 1
            log.exception("callback failed", extra={"url": request.url})
            await self._dead_letter(
                request, f"callback_error:{type(exc).__name__}", error=repr(exc)
            )

    async def _handle_output(self, output: Any, response: Response) -> None:
        if output is None:
            return
        if isinstance(output, Request):
            output.depth = response.request.depth + 1
            await self._schedule(output)
            return
        if isinstance(output, BaseModel):
            await self._emit(output.model_dump(mode="json"))
            return
        if isinstance(output, dict):
            model = self.spider.item_model
            if model is None:
                await self._emit(output)
                return
            try:
                validated = model.model_validate(output)
            except ValidationError as exc:
                self.stats.rejects += 1
                self.metrics.rejects.labels(self.spider.name).inc()
                errors = json.loads(exc.json(include_url=False))
                log.info("item rejected", extra={"url": response.url, "errors": errors})
                await self.rejects.put(
                    output,
                    errors,
                    url=response.url,
                    spider=self.spider.name,
                    schema=model.__name__,
                )
                return
            await self._emit(validated.model_dump(mode="json"))
            return
        raise TypeError(f"callbacks must yield Request, dict or BaseModel, not {type(output)}")

    async def _emit(self, item: dict[str, Any]) -> None:
        self.stats.items += 1
        self.metrics.items.labels(self.spider.name).inc()
        for sink in self.sinks:
            try:
                await sink.write(item)
            except Exception:
                self.stats.sink_errors += 1
                log.exception("sink write failed", extra={"sink": sink.name})

    # ------------------------------------------------------------------ monitoring

    async def _monitor(self) -> None:
        last_progress = self.clock.monotonic()
        while True:
            await asyncio.sleep(1.0)
            self.metrics.frontier_size.set(await self.frontier.size())
            self.metrics.in_flight.set(len(self._active))
            self.metrics.deferred.set(len(self._deferred))
            for domain, state in self.breakers.states().items():
                self.metrics.breaker_state.labels(domain).set(int(state))
            now = self.clock.monotonic()
            if now - last_progress >= self.settings.progress_interval:
                last_progress = now
                log.info(
                    "progress",
                    extra={
                        "requests": self.stats.requests,
                        "items": self.stats.items,
                        "retries": self.stats.retries,
                        "dead_letters": self.stats.dead_letters,
                        "frontier": await self.frontier.size(),
                        "in_flight": len(self._active),
                    },
                )
