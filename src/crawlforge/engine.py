"""The crawl engine: workers pulling from the frontier through politeness and resilience layers.

Request lifecycle::

    frontier.receive (next request of a ready domain) -> robots check -> circuit breaker ->
    per-domain slot (semaphore + token bucket) -> fetch (HTTP or browser) -> classify ->
    retry / dead-letter / callback -> items (validate -> sinks | rejects) and follow-up
    requests (-> frontier)

Scheduling is per domain: a worker asks the frontier for the next request of a domain it can
send to right now, skipping domains that are out of rate tokens, at their concurrency cap or
behind an open circuit breaker. Their requests wait in the queue instead of holding a
worker, so one slow domain can't stall the others (no head-of-line blocking). When every
domain with queued work is blocked, one timer wakes the workers when the first of them
becomes ready.

Retries are *deferred*: a timer task re-queues them later, so a worker is never parked on a
backoff sleep.

With a leasing backend (Redis) every popped request is leased: the lease is extended while
the request is processed or deferred, acked once it is done (callback run, or dead-lettered),
swapped for the re-queued request when a retry fires, and released on shutdown. Expired
leases of any process are swept back into the queue, so several processes can share one
frontier.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from crawlforge.breaker import BreakerRegistry, BreakerState, CircuitBreaker
from crawlforge.checkpoint import Checkpoint
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
        renderer: Fetcher | None = None,
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
        # Browser rendering is created lazily on the first ``render=True`` request.
        self.renderer: Fetcher | None = renderer
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
        # Domains of active requests that haven't taken their rate token yet, by id(request).
        self._unpaid: dict[int, str] = {}
        # Lease ids of requests this process holds (active or deferred), by id(request).
        self._leases: dict[int, str] = {}
        # Held leases another process reclaimed; kept so a late ack/requeue is a no-op.
        self._lost: set[str] = set()
        self._deferred: dict[asyncio.Task[None], Request] = {}
        self._crawl_delay_checked: set[str] = set()
        self._dispatched = 0
        self._idle = 0
        self._stopping = False
        self._stop_reason: str | None = None
        self._done = asyncio.Event()
        self._wakeup = asyncio.Event()
        # Bumped by every wakeup, so a worker that missed the event still notices it.
        self._signals = 0
        # Wakes the workers when the first blocked domain with queued work becomes ready.
        self._timer: asyncio.Task[None] | None = None
        self._timer_due = 0.0
        # Serializes frontier mutations (pop+register, push) against checkpoint capture so a
        # snapshot never misses a request that is moving between queue, workers and timers.
        self._lock = asyncio.Lock()
        self.checkpoint = Checkpoint(s.checkpoint_path) if s.checkpoint_path else None

    # ------------------------------------------------------------------ control

    def stop(self, reason: str = "stopped") -> None:
        """Graceful stop: finish in-flight requests, keep the rest for a resume."""
        if not self._stopping:
            log.info("stopping crawl", extra={"reason": reason})
            self._stopping = True
            self._stop_reason = reason
            self._wake()

    def _wake(self) -> None:
        self._signals += 1
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
            if self.frontier.backend.leasing:
                background.append(asyncio.create_task(self._keep_leases()))
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
        s = self.settings
        backend = self.frontier.backend
        if s.resume and self.checkpoint is not None and self.checkpoint.exists():
            data = self.checkpoint.load(spider=self.spider.name)
            async with self._lock:
                await backend.restore(data["frontier"])
            self.stats.absorb(data.get("stats", {}))
            self.stats.resumed = True
            log.info(
                "resumed from checkpoint",
                extra={
                    "path": str(self.checkpoint.path),
                    "pending": len(data["frontier"]["pending"]),
                    "seen": len(data["frontier"]["seen"]),
                },
            )
            return
        if s.resume and backend.persistent:
            # Leased requests count too: another process may still be working (or have died
            # holding leases that will expire), and clearing would wipe its frontier.
            pending, in_flight = await backend.size(), await backend.in_flight()
            if pending or in_flight:
                self.stats.resumed = True
                log.info(
                    "resumed from persistent queue",
                    extra={"pending": pending, "in_flight": in_flight},
                )
                return
        if s.resume:
            log.info("nothing to resume; starting fresh")
        await backend.clear()
        start = self.spider.start_requests()
        if hasattr(start, "__aiter__"):
            async for request in start:
                await self._schedule(request)
        else:
            for request in start:
                await self._schedule(request)

    async def _shutdown(self, error: BaseException | None) -> None:
        self._stopping = True
        if self._timer is not None:
            self._timer.cancel()
        # Anything not finished goes back to the frontier so a checkpoint keeps it.
        leftovers = list(self._deferred.items())
        for task, _ in leftovers:
            task.cancel()
        await asyncio.gather(*(t for t, _ in leftovers), return_exceptions=True)
        backend = self.frontier.backend
        async with self._lock:
            # Deferred requests carry an updated retry count: swap them in for their lease.
            for _, request in leftovers:
                await backend.requeue(self._leases.pop(id(request), None), request)
            active = list(self._active.items())
            if backend.leasing:
                # Released right away (not left to expire), without counting as a delivery.
                leases = [self._leases.pop(key) for key, _ in active if key in self._leases]
                released = await backend.release(leases)
                if released:
                    log.info("released leases", extra={"count": released})
            else:
                for _, request in active:
                    await self.frontier.push(request, force=True)
            self._active.clear()
            self._unpaid.clear()
            self._deferred.clear()
            self._leases.clear()
            self._lost.clear()

        remaining = await self.frontier.size()
        if error is not None:
            self.stats.finish_reason = f"error: {type(error).__name__}"
        elif self._stop_reason:
            self.stats.finish_reason = self._stop_reason
        else:
            self.stats.finish_reason = "finished" if remaining == 0 else "stopped"

        if self.checkpoint is not None:
            if remaining == 0 and error is None:
                self.checkpoint.remove()
            else:
                await self.save_checkpoint()
                log.info(
                    "checkpoint saved",
                    extra={"path": str(self.checkpoint.path), "pending": remaining},
                )

        for sink in self.sinks:
            try:
                await sink.close()
            except Exception:
                log.exception("sink close failed", extra={"sink": sink.name})
                self.stats.sink_errors += 1
        await self.dlq.sink.close()
        await self.rejects.sink.close()
        await self.fetcher.close()
        if self.renderer is not None:
            await self.renderer.close()
        await self.frontier.backend.close()

    # ------------------------------------------------------------------ scheduling

    async def _schedule(self, request: Request, *, force: bool = False) -> PushResult:
        async with self._lock:
            result = await self.frontier.push(request, force=force)
        if result is PushResult.DUPLICATE:
            self.stats.duplicates += 1
            self.metrics.dropped.labels("duplicate").inc()
        elif result is PushResult.TOO_DEEP:
            self.stats.too_deep += 1
            self.metrics.dropped.labels("too_deep").inc()
        else:
            self._wake()
        return result

    def _defer(self, request: Request, delay: float) -> None:
        """Re-queue ``request`` after ``delay``. Its lease (if any) is held until then."""

        async def requeue() -> None:
            await self.clock.sleep(delay)
            async with self._lock:
                self._deferred.pop(task, None)
                lease = self._leases.pop(id(request), None)
                if not await self.frontier.backend.requeue(lease, request):
                    self._lease_lost(request, lease)
                self._lost.discard(lease or "")
            self._wake()

        task = asyncio.create_task(requeue())
        self._deferred[task] = request
        task.add_done_callback(lambda t: self._deferred.pop(t, None))

    async def _worker(self) -> None:
        s = self.settings
        while not self._stopping and not self._done.is_set():
            if s.max_requests is not None and self._dispatched >= s.max_requests:
                self.stop("max_requests")
                return
            signals = self._signals
            async with self._lock:
                checked = self.clock.monotonic()
                blocked = self._blocked()
                delivery = await self.frontier.receive(s.lease_ttl, skip=blocked)
                if delivery is not None:
                    self._dispatched += 1
                    request = delivery.request
                    key = id(request)
                    self._active[key] = request
                    self._unpaid[key] = request.domain
                    if delivery.lease is not None:
                        self._leases[key] = delivery.lease
            if delivery is None:
                if blocked:
                    await self._wake_when_ready(blocked, checked)
                if await self._wait_for_work(signals):
                    return
                continue
            try:
                if delivery.deliveries > s.max_deliveries:
                    # Every earlier holder let the lease expire (crash, hang, OOM kill...).
                    await self._dead_letter(request, "lease_expired_too_often")
                else:
                    await self._process(request)
            except asyncio.CancelledError:
                # Leave it in _active: _shutdown re-queues it for the checkpoint.
                raise
            except Exception as exc:
                log.exception("internal error processing request", extra={"url": request.url})
                await self._dead_letter(request, "internal_error", error=repr(exc))
            await self._settle(key, request)

    async def _settle(self, key: int, request: Request) -> None:
        """Done with a popped request: ack its lease unless a timer now owns it (deferred)."""
        async with self._lock:
            self._active.pop(key, None)
            self._unpaid.pop(key, None)
            # Its domain may be under its concurrency cap (or past its probe) again.
            self._wake()
            if any(r is request for r in self._deferred.values()):
                return
            lease = self._leases.pop(key, None)
            if lease is not None:
                if not await self.frontier.backend.ack(lease):
                    self._lease_lost(request, lease)
                self._lost.discard(lease)

    def _lease_lost(self, request: Request, lease: str | None) -> None:
        # The lease expired and another process reclaimed it; the request will be (or was)
        # fetched again there. Lease TTL too short for the slowest requests?
        if lease is None or lease in self._lost:
            return
        self._lost.add(lease)
        self.stats.leases_lost += 1
        log.warning("lease lost", extra={"url": request.url, "lease_ttl": self.settings.lease_ttl})

    def _blocked(self) -> dict[str, float]:
        """Domains this process can't send to now -> seconds until they may be ready.

        ``inf`` means "until one of the domain's active requests finishes": settling wakes
        the workers, no timer is needed.
        """
        active = Counter(r.domain for r in self._active.values())
        blocked = self.throttle.throttled(Counter(self._unpaid.values()))
        for domain, n in active.items():
            if n >= self.throttle.concurrency:
                blocked[domain] = math.inf
        for domain, wait in self.breakers.blocked(active).items():
            blocked[domain] = max(blocked.get(domain, 0.0), wait)
        return blocked

    async def _wake_when_ready(self, blocked: dict[str, float], checked: float) -> None:
        """Nothing was ready: arm the timer for the first blocked domain that has work.

        ``blocked`` holds waits measured at ``checked``; the queue lookup may take a while
        (Redis), so the timer is set for that absolute time, not for the wait.
        """
        timed = [domain for domain, wait in blocked.items() if wait != math.inf]
        if not timed:
            return
        waiting = await self.frontier.backend.queued_domains(timed)
        if not waiting:
            return
        due = checked + min(blocked[domain] for domain in waiting)
        delay = due - self.clock.monotonic()
        if delay <= 0:
            self._wake()  # ready already (another timer got there first)
            return
        if self._timer is not None and not self._timer.done():
            if self._timer_due <= due:
                return
            self._timer.cancel()

        async def ring() -> None:
            await self.clock.sleep(delay)
            self._wake()

        self._timer_due = due
        self._timer = asyncio.create_task(ring())

    async def _wait_for_work(self, signals: int) -> bool:
        """Idle until new work may exist. Returns True when the crawl is complete.

        ``signals`` is the wakeup count seen before the frontier was found empty (or
        blocked); a wakeup since then means there may be work again, so don't wait.
        """
        self._idle += 1
        try:
            if (
                self._idle == self.settings.concurrency
                and not self._active
                and not self._deferred
                and await self._frontier_drained()
            ):
                self._done.set()
                self._wake()
                return True
            if self._signals != signals:
                return self._done.is_set()
            self._wakeup.clear()
            try:
                async with asyncio.timeout(_IDLE_POLL):
                    await self._wakeup.wait()
            except TimeoutError:
                pass
            return self._done.is_set()
        finally:
            self._idle -= 1

    async def _frontier_drained(self) -> bool:
        if await self.frontier.size() > 0:
            return False
        backend = self.frontier.backend
        if not backend.leasing:
            return True
        # Other processes may still yield follow-ups, or have died with expired leases.
        if await self._reclaim():
            return False
        return await backend.in_flight() == 0

    async def _reclaim(self) -> int:
        reclaimed = await self.frontier.backend.reclaim()
        if reclaimed:
            self.stats.leases_reclaimed += reclaimed
            self.metrics.leases_reclaimed.inc(reclaimed)
            log.warning("reclaimed expired leases", extra={"count": reclaimed})
            self._wake()
        return reclaimed

    async def _keep_leases(self) -> None:
        """Extend this process's leases and sweep expired ones, every third of the TTL."""
        ttl = self.settings.lease_ttl
        backend = self.frontier.backend
        while True:
            await asyncio.sleep(ttl / 3)
            async with self._lock:
                held = {v: k for k, v in self._leases.items() if v not in self._lost}
                lost = await backend.extend(held, ttl)
            for lease in lost:
                key = held[lease]
                request = self._active.get(key) or next(
                    (r for r in self._deferred.values() if id(r) == key), None
                )
                if request is not None:
                    self._lease_lost(request, lease)
            await self._reclaim()

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
                self._unpaid.pop(id(request), None)
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
        fetcher = self._get_renderer() if request.render else self.fetcher
        async with asyncio.timeout(self.settings.timeout):
            return await fetcher.fetch(request, timeout=self.settings.timeout)

    def _get_renderer(self) -> Fetcher:
        if self.renderer is None:
            from crawlforge.render import PlaywrightRenderer

            self.renderer = PlaywrightRenderer(
                self.settings.user_agent, concurrency=self.settings.render_concurrency
            )
        return self.renderer

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

    # ------------------------------------------------------------------ checkpoints

    async def save_checkpoint(self) -> None:
        """Persist queue + in-flight + deferred requests and the seen set.

        In-flight requests are included so a crash re-fetches them (at-least-once).
        """
        if self.checkpoint is None:
            return
        async with self._lock:
            held = (*self._active.values(), *self._deferred.values())
            # Leased requests are already in a leasing backend's snapshot.
            extra = [r.to_dict() for r in held if id(r) not in self._leases]
            snapshot = await self.frontier.backend.snapshot()
        snapshot["pending"] = snapshot["pending"] + extra
        stats = self.stats.to_dict()
        self.checkpoint.save(spider=self.spider.name, frontier=snapshot, stats=stats)

    # ------------------------------------------------------------------ monitoring

    async def _monitor(self) -> None:
        last_progress = last_checkpoint = self.clock.monotonic()
        while True:
            await asyncio.sleep(1.0)
            now = self.clock.monotonic()
            if (
                self.checkpoint is not None
                and now - last_checkpoint >= self.settings.checkpoint_interval
            ):
                last_checkpoint = now
                await self.save_checkpoint()
            self.metrics.frontier_size.set(await self.frontier.size())
            self.metrics.in_flight.set(len(self._active))
            self.metrics.deferred.set(len(self._deferred))
            for domain, state in self.breakers.states().items():
                self.metrics.breaker_state.labels(domain).set(int(state))
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
