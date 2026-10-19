"""Re-queue dead letters after a fix (``crawlforge replay-dlq``).

The DLQ file (``dlq.jsonl``) holds one JSON entry per permanently failed request, each with
the full serialized request. Replaying selects entries with filters, resets their retry
budget and pushes them into the place the next ``crawlforge run --resume`` reads from:

- a checkpoint file: requests are appended to ``frontier.pending``. Pending requests are
  restored without a dedupe check, so they bypass the seen-set; their fingerprints are
  added to ``seen`` so links back to them stay de-duplicated. If the file does not exist
  (the crawl finished and removed it), a checkpoint holding only the replayed requests is
  created;
- a Redis queue: requests are pushed straight onto the queue sorted set (again without a
  dedupe check) and their fingerprints are added to the seen set. Requests that are queued
  or leased (in flight in a running process) count as already pending.

Replayed entries are marked in the DLQ file with ``replayed_at`` (the file is rewritten
atomically), so replaying twice never queues the same entry twice. Failing again after a
replay appends a fresh, unmarked entry, which can be replayed again.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from crawlforge.checkpoint import Checkpoint
from crawlforge.http import Request
from crawlforge.queues.base import FrontierSnapshot, QueueBackend

_DURATION = re.compile(r"^(\d+(?:\.\d+)?)([smhdw])$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_since(value: str, *, now: datetime | None = None) -> datetime:
    """Parse ``--since``: an ISO 8601 timestamp (UTC if naive) or a duration like ``2h``."""
    value = value.strip()
    if match := _DURATION.match(value):
        amount, unit = match.groups()
        return (now or datetime.now(UTC)) - timedelta(**{_UNITS[unit]: float(amount)})
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(
            f"invalid --since {value!r}: use an ISO timestamp or a duration like 30m, 2h, 7d"
        ) from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass
class DlqFilter:
    """Selects dead letters. Every given criterion must match (logical AND)."""

    reason: str | None = None
    regex: bool = False
    domains: list[str] = field(default_factory=list)
    since: datetime | None = None

    def __post_init__(self) -> None:
        self._pattern = re.compile(self.reason) if self.reason and self.regex else None
        self.domains = [d.lower().lstrip(".") for d in self.domains]

    def matches(self, entry: dict[str, Any], request: Request) -> bool:
        reason = str(entry.get("reason") or "")
        if self._pattern is not None:
            if not self._pattern.search(reason):
                return False
        elif self.reason and self.reason not in reason:
            return False
        if self.domains:
            # Port-insensitive host match; "example.org" also matches its subdomains.
            host = request.domain.split(":", 1)[0]
            if not any(host == d or host.endswith(f".{d}") for d in self.domains):
                return False
        if self.since is not None:
            ts = _parse_ts(entry.get("ts"))
            if ts is None or ts < self.since:
                return False
        return True


@dataclass
class DlqLine:
    """One line of the DLQ file. ``entry``/``request`` are ``None`` if it can't be parsed."""

    raw: str
    entry: dict[str, Any] | None = None
    request: Request | None = None


def read_dlq(path: Path) -> list[DlqLine]:
    lines: list[DlqLine] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        line = DlqLine(raw)
        try:
            entry = json.loads(raw)
            line.request = Request.from_dict(entry["request"])
            line.entry = entry
        except (ValueError, KeyError, TypeError):
            pass
        lines.append(line)
    return lines


def write_dlq(path: Path, lines: list[DlqLine]) -> None:
    """Rewrite the DLQ atomically, keeping unparseable lines verbatim."""
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for line in lines:
            if line.entry is None:
                fh.write(line.raw + "\n")
            else:
                fh.write(json.dumps(line.entry, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


@dataclass
class ReplayPlan:
    total: int = 0
    matched: list[DlqLine] = field(default_factory=list)
    selected: list[DlqLine] = field(default_factory=list)
    #: Matching entries that are not replayed, by reason.
    skipped: Counter[str] = field(default_factory=Counter)
    #: Lines that are not valid dead-letter entries (never matched, never touched).
    unreadable: int = 0
    #: Matching entries whose request is already covered by a selected or queued one;
    #: they are marked as replayed along with it.
    covered: list[DlqLine] = field(default_factory=list)


def plan_replay(
    lines: list[DlqLine],
    flt: DlqFilter,
    *,
    limit: int | None = None,
    queued: set[str] | frozenset[str] = frozenset(),
) -> ReplayPlan:
    """Decide which entries to replay. ``queued`` holds fingerprints already pending."""
    plan = ReplayPlan(total=len(lines))
    picked: set[str] = set()
    for line in lines:
        if line.entry is None or line.request is None:
            plan.unreadable += 1
            continue
        if not flt.matches(line.entry, line.request):
            continue
        plan.matched.append(line)
        fp = line.request.fingerprint
        if line.entry.get("replayed_at"):
            plan.skipped["already replayed"] += 1
        elif fp in queued:
            plan.skipped["already queued"] += 1
            plan.covered.append(line)
        elif fp in picked:
            plan.skipped["duplicate entry"] += 1
            plan.covered.append(line)
        elif limit is not None and len(plan.selected) >= limit:
            plan.skipped["over --limit"] += 1
        else:
            picked.add(fp)
            plan.selected.append(line)
    return plan


def replay_request(line: DlqLine) -> Request:
    """The request to re-queue: same URL/callback/meta/depth, fresh retry budget."""
    assert line.request is not None
    request = Request.from_dict(line.request.to_dict())
    request.retries = 0
    return request


def mark_replayed(marked: list[DlqLine], target: str) -> None:
    stamp = datetime.now(UTC).isoformat(timespec="milliseconds")
    for line in marked:
        assert line.entry is not None
        line.entry["replayed_at"] = stamp
        line.entry["replayed_to"] = target


def checkpoint_pending(checkpoint: Checkpoint, spider: str) -> set[str]:
    if not checkpoint.exists():
        return set()
    data = checkpoint.load(spider=spider)
    return {Request.from_dict(p).fingerprint for p in data["frontier"]["pending"]}


def replay_into_checkpoint(checkpoint: Checkpoint, spider: str, requests: list[Request]) -> None:
    """Append ``requests`` to the checkpoint's pending list (creating it if needed)."""
    if checkpoint.exists():
        data = checkpoint.load(spider=spider)
        frontier: FrontierSnapshot = data["frontier"]
        stats = data.get("stats", {})
    else:
        frontier = {"pending": [], "seen": []}
        stats = {}
    seen = set(frontier["seen"])
    seen.update(r.fingerprint for r in requests)
    frontier = {
        "pending": frontier["pending"] + [r.to_dict() for r in requests],
        "seen": sorted(seen),
    }
    checkpoint.save(spider=spider, frontier=frontier, stats=stats)


async def queue_pending(backend: QueueBackend) -> set[str]:
    """Fingerprints of requests queued or in flight (leased) in a persistent backend."""
    snapshot = await backend.snapshot()
    return {Request.from_dict(p).fingerprint for p in snapshot["pending"]}


async def replay_into_queue(backend: QueueBackend, requests: list[Request]) -> None:
    """Push ``requests`` onto a (persistent) queue backend, bypassing its dedupe check."""
    for request in requests:
        await backend.add_seen(request.fingerprint)
        await backend.push(request)
