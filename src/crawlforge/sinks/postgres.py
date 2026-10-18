"""PostgreSQL sink: batched upserts of items into a JSONB table keyed by one field."""

from __future__ import annotations

import json
import re
from typing import Any

from crawlforge.sinks.base import Sink

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class PostgresSink(Sink):
    """Upserts items into ``table(key TEXT PRIMARY KEY, data JSONB, scraped_at TIMESTAMPTZ)``.

    The primary key is ``str(item[key])``. Re-crawling the same entity updates
    the row instead of duplicating it, which makes re-runs and at-least-once
    delivery after a crash idempotent.
    """

    name = "postgres"

    def __init__(
        self,
        dsn: str,
        *,
        table: str = "items",
        key: str = "url",
        batch_size: int = 100,
        create_table: bool = True,
    ) -> None:
        if not _IDENT.match(table):
            raise ValueError(f"invalid table name: {table!r}")
        self.dsn = dsn
        self.table = table
        self.key = key
        self.batch_size = batch_size
        self.create_table = create_table
        self._pool: Any = None
        self._buffer: dict[str, str] = {}
        self.count = 0

    async def open(self) -> None:
        import asyncpg

        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4)
        if self.create_table:
            await self._pool.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    scraped_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )

    async def write(self, item: dict[str, Any]) -> None:
        if self.key not in item or item[self.key] in (None, ""):
            raise KeyError(f"item has no value for upsert key {self.key!r}")
        # Keyed buffer: a later item with the same key replaces the earlier one.
        self._buffer[str(item[self.key])] = json.dumps(item, ensure_ascii=False, default=str)
        self.count += 1
        if len(self._buffer) >= self.batch_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer or self._pool is None:
            return
        rows = list(self._buffer.items())
        self._buffer.clear()
        await self._pool.executemany(
            f"""
            INSERT INTO {self.table} (key, data, scraped_at) VALUES ($1, $2::jsonb, now())
            ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, scraped_at = now()
            """,
            rows,
        )

    async def close(self) -> None:
        await self.flush()
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
