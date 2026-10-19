"""Crawler settings: defaults < spider.custom_settings < explicit overrides (CLI)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from crawlforge import __version__

DEFAULT_USER_AGENT = f"CrawlForge/{__version__} (+https://github.com/huss-huss/crawlforge)"


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # Identity & politeness
    user_agent: str = DEFAULT_USER_AGENT
    obey_robots: bool = True
    concurrency: int = Field(8, ge=1, description="Global worker count.")
    per_domain_concurrency: int = Field(2, ge=1)
    rate_per_domain: float = Field(2.0, gt=0, description="Requests per second per domain.")
    burst: float = Field(2.0, ge=1)

    # Fetching
    timeout: float = Field(30.0, gt=0, description="Total per-attempt timeout in seconds.")
    follow_redirects: bool = True
    handle_statuses: list[int] = Field(
        default_factory=list, description="Non-2xx/3xx statuses passed to callbacks."
    )
    render_concurrency: int = Field(2, ge=1, description="Max concurrent browser pages.")

    # Resilience
    max_retries: int = Field(3, ge=0)
    backoff_base: float = Field(0.5, ge=0)
    backoff_cap: float = Field(30.0, ge=0)
    max_retry_after: float = Field(300.0, ge=0)
    breaker_threshold: int = Field(5, ge=1)
    breaker_recovery: float = Field(30.0, ge=0)

    # Shared (leasing) queues, e.g. Redis with several crawler processes
    lease_ttl: float = Field(
        60.0, gt=0, description="Seconds a popped request stays leased without an extension."
    )
    max_deliveries: int = Field(
        3, ge=1, description="Times a request may be leased out before it is dead-lettered."
    )

    # Scope
    max_depth: int | None = Field(None, ge=0)
    max_requests: int | None = Field(None, ge=1, description="Stop after N dispatched requests.")

    # Outputs & state
    output_dir: Path = Path(".crawlforge")
    dlq_path: Path | None = None
    rejects_path: Path | None = None
    stats_path: Path | None = None
    checkpoint_path: Path | None = None
    checkpoint_interval: float = Field(30.0, gt=0)
    resume: bool = False

    # Observability
    metrics_port: int | None = None
    metrics_addr: str = "0.0.0.0"
    progress_interval: float = Field(10.0, gt=0)

    def resolved(self, name: str) -> Path:
        """Path for ``dlq``/``rejects``/``stats``, defaulting into ``output_dir``."""
        explicit = getattr(self, f"{name}_path")
        if explicit is not None:
            return Path(explicit)
        return self.output_dir / {"dlq": "dlq.jsonl", "rejects": "rejects.jsonl"}.get(
            name, f"{name}.json"
        )

    @classmethod
    def build(cls, *layers: dict[str, Any] | None) -> Settings:
        merged: dict[str, Any] = {}
        for layer in layers:
            if layer:
                merged.update({k: v for k, v in layer.items() if v is not None})
        return cls(**merged)
