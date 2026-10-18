"""Line-delimited JSON and CSV file sinks."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import IO, Any

from crawlforge.sinks.base import Sink


def _json_default(value: Any) -> Any:
    # Pydantic's mode="json" dump already handles most types; this covers
    # anything a user passes through as a plain dict.
    if isinstance(value, set | frozenset):
        return sorted(value)
    return str(value)


class JsonlSink(Sink):
    """One JSON object per line. ``append=True`` keeps existing content (used on resume)."""

    name = "jsonl"

    def __init__(self, path: str | Path, *, append: bool = False) -> None:
        self.path = Path(path)
        self.append = append
        self._fh: IO[str] | None = None
        self.count = 0

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a" if self.append else "w", encoding="utf-8")

    async def write(self, item: dict[str, Any]) -> None:
        if self._fh is None:
            await self.open()
        assert self._fh is not None
        self._fh.write(json.dumps(item, ensure_ascii=False, default=_json_default) + "\n")
        self.count += 1

    async def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


class CsvSink(Sink):
    """CSV with a header taken from ``fields`` or the first item's keys.

    Nested values (lists/dicts) are JSON-encoded into the cell. Keys that are
    not in the header are dropped, so declare ``fields`` if items vary.
    """

    name = "csv"

    def __init__(
        self, path: str | Path, *, fields: list[str] | None = None, append: bool = False
    ) -> None:
        self.path = Path(path)
        self.fields = list(fields) if fields else None
        self.append = append
        self._fh: IO[str] | None = None
        self._writer: csv.DictWriter[str] | None = None
        self.count = 0

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        resume = self.append and self.path.exists() and self.path.stat().st_size > 0
        if resume and self.fields is None:
            with self.path.open(newline="", encoding="utf-8") as fh:
                self.fields = next(csv.reader(fh), None)
        self._fh = self.path.open("a" if resume else "w", newline="", encoding="utf-8")
        self._header_written = resume

    def _ensure_writer(self, item: dict[str, Any]) -> csv.DictWriter[str]:
        if self._writer is None:
            assert self._fh is not None
            if self.fields is None:
                self.fields = list(item.keys())
            self._writer = csv.DictWriter(self._fh, fieldnames=self.fields, extrasaction="ignore")
            if not self._header_written:
                self._writer.writeheader()
                self._header_written = True
        return self._writer

    async def write(self, item: dict[str, Any]) -> None:
        if self._fh is None:
            await self.open()
        writer = self._ensure_writer(item)
        row = {
            k: json.dumps(v, ensure_ascii=False) if isinstance(v, list | dict) else v
            for k, v in item.items()
        }
        writer.writerow(row)
        self.count += 1

    async def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
            self._writer = None
