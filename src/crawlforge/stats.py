"""Run statistics persisted at the end of (and during) a crawl."""

from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CrawlStats:
    spider: str = ""
    requests: int = 0
    responses: int = 0
    items: int = 0
    rejects: int = 0
    retries: int = 0
    dead_letters: int = 0
    duplicates: int = 0
    too_deep: int = 0
    robots_blocked: int = 0
    fetch_errors: int = 0
    callback_errors: int = 0
    sink_errors: int = 0
    breaker_trips: int = 0
    leases_reclaimed: int = 0
    leases_lost: int = 0
    status_counts: Counter[str] = field(default_factory=Counter)
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_seconds: float = 0.0
    finish_reason: str | None = None
    resumed: bool = False

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status_counts"] = dict(sorted(self.status_counts.items()))
        return data

    def absorb(self, previous: dict[str, Any]) -> None:
        """Add counters from an earlier (checkpointed) run so totals span a resume."""
        for name, value in previous.items():
            if name == "status_counts":
                self.status_counts.update(value)
            elif isinstance(value, int) and not isinstance(value, bool):
                current = getattr(self, name, None)
                if isinstance(current, int) and not isinstance(current, bool):
                    setattr(self, name, current + value)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.to_dict(), indent=2))
        os.replace(tmp, path)
