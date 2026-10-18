"""Dead-letter queue for failed requests and a rejects sink for invalid items."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from crawlforge.http import Request
from crawlforge.sinks.base import Sink


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class DeadLetterQueue:
    """Records requests that permanently failed, with the reason.

    Entries embed the full serialized request, so they can be inspected or
    re-queued later.
    """

    def __init__(self, sink: Sink) -> None:
        self.sink = sink
        self.count = 0

    async def put(
        self,
        request: Request,
        reason: str,
        *,
        status: int | None = None,
        error: str | None = None,
    ) -> None:
        self.count += 1
        await self.sink.write(
            {
                "ts": _now(),
                "reason": reason,
                "status": status,
                "error": error,
                "attempts": request.retries + 1,
                "request": request.to_dict(),
            }
        )


class RejectsSink:
    """Records items that failed schema validation."""

    def __init__(self, sink: Sink) -> None:
        self.sink = sink
        self.count = 0

    async def put(
        self, item: Any, errors: list[dict[str, Any]], *, url: str, spider: str, schema: str
    ) -> None:
        self.count += 1
        await self.sink.write(
            {
                "ts": _now(),
                "spider": spider,
                "schema": schema,
                "url": url,
                "errors": errors,
                "item": item,
            }
        )
