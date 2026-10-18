"""Checkpoint files: the frontier (pending requests + seen fingerprints) and stats on disk."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from crawlforge.queues.base import FrontierSnapshot

FORMAT_VERSION = 1


class Checkpoint:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def exists(self) -> bool:
        return self.path.exists()

    def save(self, *, spider: str, frontier: FrontierSnapshot, stats: dict[str, Any]) -> None:
        """Write atomically (temp file + rename) so a crash never leaves a torn checkpoint."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": FORMAT_VERSION,
            "spider": spider,
            "saved_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "frontier": frontier,
            "stats": stats,
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def load(self, *, spider: str) -> dict[str, Any]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if data.get("version") != FORMAT_VERSION:
            raise ValueError(f"unsupported checkpoint version {data.get('version')!r}")
        if data.get("spider") != spider:
            raise ValueError(
                f"checkpoint {self.path} belongs to spider {data.get('spider')!r}, not {spider!r}"
            )
        return data

    def remove(self) -> None:
        self.path.unlink(missing_ok=True)
