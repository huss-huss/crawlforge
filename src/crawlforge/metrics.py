"""Prometheus metrics. Each crawler owns its own registry (no global state)."""

from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)

LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.requests = Counter(
            "crawlforge_requests", "Requests sent (each attempt counts).", ["domain"], registry=r
        )
        self.responses = Counter(
            "crawlforge_responses", "Responses received by status code.",
            ["domain", "status"], registry=r,
        )  # fmt: skip
        self.fetch_errors = Counter(
            "crawlforge_fetch_errors", "Transport-level failures by exception type.",
            ["domain", "error"], registry=r,
        )  # fmt: skip
        self.retries = Counter(
            "crawlforge_retries", "Retries scheduled.", ["domain", "reason"], registry=r
        )
        self.items = Counter("crawlforge_items", "Items accepted.", ["spider"], registry=r)
        self.rejects = Counter(
            "crawlforge_item_rejects", "Items that failed schema validation.", ["spider"],
            registry=r,
        )  # fmt: skip
        self.dead_letters = Counter(
            "crawlforge_dead_letters", "Requests sent to the dead-letter queue.",
            ["domain", "reason"], registry=r,
        )  # fmt: skip
        self.dropped = Counter(
            "crawlforge_requests_dropped", "Requests not scheduled (duplicate, too deep, robots).",
            ["reason"], registry=r,
        )  # fmt: skip
        self.latency = Histogram(
            "crawlforge_request_latency_seconds", "Fetch latency.", ["domain"],
            buckets=LATENCY_BUCKETS, registry=r,
        )  # fmt: skip
        self.breaker_state = Gauge(
            "crawlforge_circuit_breaker_state",
            "Circuit breaker state per domain (0=closed, 1=half-open, 2=open).",
            ["domain"], registry=r,
        )  # fmt: skip
        self.breaker_trips = Counter(
            "crawlforge_circuit_breaker_trips", "Times a breaker opened.", ["domain"], registry=r
        )
        self.frontier_size = Gauge(
            "crawlforge_frontier_size", "Requests waiting in the frontier.", registry=r
        )
        self.in_flight = Gauge(
            "crawlforge_in_flight", "Requests currently being processed.", registry=r
        )
        self.deferred = Gauge(
            "crawlforge_deferred", "Requests waiting on backoff or an open breaker.", registry=r
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def serve(self, port: int, addr: str = "0.0.0.0") -> None:
        """Expose ``/metrics`` on ``addr:port`` from a background thread."""
        start_http_server(port, addr=addr, registry=self.registry)
