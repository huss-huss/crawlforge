"""Structured JSON logging on top of the standard library."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_RESERVED = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """Renders records as one JSON object per line.

    Anything passed via ``extra={...}`` becomes a top-level field, so
    ``log.info("fetched", extra={"url": u, "status": 200})`` is queryable.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO", *, json_logs: bool = True) -> None:
    handler = logging.StreamHandler(sys.stderr)
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s"))
    root = logging.getLogger("crawlforge")
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    root.propagate = False
