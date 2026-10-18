"""Redis-backed frontier (sorted set queue + set for fingerprints)."""

from __future__ import annotations

import json
from typing import Any

from crawlforge.http import Request
from crawlforge.queues.base import FrontierSnapshot, QueueBackend

# Scores encode (priority desc, insertion asc) in one float. Doubles are exact
# up to 2**53 (~9e15), so with a 1e12 sequence window priorities in [-1000, 1000]
# stay exact.
_SEQ_SPACE = 1_000_000_000_000
_MAX_PRIORITY = 1000


class RedisQueue(QueueBackend):
    persistent = True

    def __init__(self, client: Any, prefix: str = "crawlforge") -> None:
        self._redis = client
        self.prefix = prefix
        self.queue_key = f"{prefix}:queue"
        self.seen_key = f"{prefix}:seen"
        self.seq_key = f"{prefix}:seq"

    @classmethod
    def from_url(cls, url: str, prefix: str = "crawlforge") -> RedisQueue:
        from redis.asyncio import Redis

        return cls(Redis.from_url(url), prefix=prefix)

    def _score(self, priority: int, seq: int) -> float:
        priority = max(-_MAX_PRIORITY, min(_MAX_PRIORITY, priority))
        return float(-priority * _SEQ_SPACE + (seq % _SEQ_SPACE))

    async def push(self, request: Request) -> None:
        seq = await self._redis.incr(self.seq_key)
        # The sequence number is part of the member so identical payloads
        # (e.g. a forced retry) never collapse into one sorted-set entry.
        member = f"{seq}|{json.dumps(request.to_dict(), separators=(',', ':'))}"
        await self._redis.zadd(self.queue_key, {member: self._score(request.priority, seq)})

    @staticmethod
    def _decode(member: bytes | str) -> Request:
        if isinstance(member, bytes):
            member = member.decode()
        _, payload = member.split("|", 1)
        return Request.from_dict(json.loads(payload))

    async def pop(self) -> Request | None:
        result = await self._redis.zpopmin(self.queue_key, 1)
        if not result:
            return None
        member, _score = result[0]
        return self._decode(member)

    async def size(self) -> int:
        return int(await self._redis.zcard(self.queue_key))

    async def add_seen(self, fp: str) -> bool:
        return bool(await self._redis.sadd(self.seen_key, fp))

    async def seen_count(self) -> int:
        return int(await self._redis.scard(self.seen_key))

    async def snapshot(self) -> FrontierSnapshot:
        members = await self._redis.zrange(self.queue_key, 0, -1)
        seen = [
            fp.decode() if isinstance(fp, bytes) else fp
            async for fp in self._redis.sscan_iter(self.seen_key, count=1000)
        ]
        return {
            "pending": [self._decode(m).to_dict() for m in members],
            "seen": sorted(seen),
        }

    async def clear(self) -> None:
        await self._redis.delete(self.queue_key, self.seen_key, self.seq_key)

    async def close(self) -> None:
        await self._redis.aclose()
