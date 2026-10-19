"""Redis-backed frontier: a sorted-set queue, a fingerprint set and leases for in-flight work.

Keys (``<prefix>`` is ``crawlforge:<spider>`` from the CLI)::

    <prefix>:queue       zset   "<seq>|<request json>" -> score (priority desc, FIFO)
    <prefix>:seen        set    request fingerprints (shared, so no URL is queued twice)
    <prefix>:seq         int    insertion counter
    <prefix>:leases      zset   lease id -> deadline (unix seconds)
    <prefix>:inflight    hash   lease id -> "<queue score>|<queue member>"
    <prefix>:deliveries  hash   queue member -> times it was handed out

``receive`` moves the best queue entry into ``leases``/``inflight`` in one Lua script, so two
processes never get the same entry. The worker then acks it (done or dead-lettered),
re-queues a retry in its place, or releases it on shutdown. A lease that is not extended
before its deadline (the process died or hung) is reclaimed by any process: the entry goes
back to the queue with its original score and keeps its delivery count, so an entry that
keeps expiring can be dead-lettered instead of being handed out forever.

Deadlines come from the clients' wall clocks, so processes sharing a queue need roughly
synchronized clocks (NTP); the lease TTL should dwarf any skew.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from crawlforge.http import Request
from crawlforge.queues.base import Delivery, FrontierSnapshot, QueueBackend

# Scores encode (priority desc, insertion asc) in one float. Doubles are exact
# up to 2**53 (~9e15), so with a 1e12 sequence window priorities in [-1000, 1000]
# stay exact.
_SEQ_SPACE = 1_000_000_000_000
_MAX_PRIORITY = 1000
_RECLAIM_BATCH = 500

# KEYS (all scripts but EXTEND): queue, leases, inflight, deliveries.

# ARGV: deadline, lease id. Returns {member, deliveries} or nil when the queue is empty.
_RECEIVE = """
local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then return false end
local member, score = popped[1], popped[2]
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[2], score .. '|' .. member)
return {member, redis.call('HINCRBY', KEYS[4], member, 1)}
"""

# Shared prologue: drop a lease and return its queue score and member (nil if it's gone).
_TAKE = """
local function take(lease)
  local entry = redis.call('HGET', KEYS[3], lease)
  redis.call('ZREM', KEYS[2], lease)
  if not entry then return nil end
  redis.call('HDEL', KEYS[3], lease)
  local cut = string.find(entry, '|', 1, true)
  return string.sub(entry, 1, cut - 1), string.sub(entry, cut + 1)
end
"""

# ARGV: lease id.
_ACK = (
    _TAKE
    + """
local score, member = take(ARGV[1])
if not score then return 0 end
redis.call('HDEL', KEYS[4], member)
return 1
"""
)

# ARGV: lease id, new member, new score. Queues nothing if the lease was lost.
_REQUEUE = (
    _TAKE
    + """
local score, member = take(ARGV[1])
if not score then return 0 end
redis.call('HDEL', KEYS[4], member)
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
return 1
"""
)

# ARGV: lease ids. The entries go back with their score; the delivery is undone.
_RELEASE = (
    _TAKE
    + """
local released = 0
for i = 1, #ARGV do
  local score, member = take(ARGV[i])
  if score then
    redis.call('ZADD', KEYS[1], score, member)
    if redis.call('HINCRBY', KEYS[4], member, -1) <= 0 then
      redis.call('HDEL', KEYS[4], member)
    end
    released = released + 1
  end
end
return released
"""
)

# KEYS: leases. ARGV: deadline, lease ids. Returns the leases that no longer exist.
_EXTEND = """
local lost = {}
for i = 2, #ARGV do
  if redis.call('ZSCORE', KEYS[1], ARGV[i]) then
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[i])
  else
    table.insert(lost, ARGV[i])
  end
end
return lost
"""

# ARGV: now, batch size. Expired entries go back with their score and delivery count.
_RECLAIM = (
    _TAKE
    + """
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local reclaimed = 0
for _, lease in ipairs(expired) do
  local score, member = take(lease)
  if score then
    redis.call('ZADD', KEYS[1], score, member)
    reclaimed = reclaimed + 1
  end
end
return {#expired, reclaimed}
"""
)


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


class RedisQueue(QueueBackend):
    persistent = True
    leasing = True

    def __init__(
        self,
        client: Any,
        prefix: str = "crawlforge",
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = client
        self.prefix = prefix
        self.queue_key = f"{prefix}:queue"
        self.seen_key = f"{prefix}:seen"
        self.seq_key = f"{prefix}:seq"
        self.leases_key = f"{prefix}:leases"
        self.inflight_key = f"{prefix}:inflight"
        self.deliveries_key = f"{prefix}:deliveries"
        # Wall clock for lease deadlines; injectable so tests can expire leases.
        self._now = clock
        self._keys = [self.queue_key, self.leases_key, self.inflight_key, self.deliveries_key]
        self._receive = client.register_script(_RECEIVE)
        self._ack = client.register_script(_ACK)
        self._requeue = client.register_script(_REQUEUE)
        self._release = client.register_script(_RELEASE)
        self._extend = client.register_script(_EXTEND)
        self._reclaim = client.register_script(_RECLAIM)

    @classmethod
    def from_url(cls, url: str, prefix: str = "crawlforge") -> RedisQueue:
        from redis.asyncio import Redis

        return cls(Redis.from_url(url), prefix=prefix)

    def _score(self, priority: int, seq: int) -> float:
        priority = max(-_MAX_PRIORITY, min(_MAX_PRIORITY, priority))
        return float(-priority * _SEQ_SPACE + (seq % _SEQ_SPACE))

    async def _entry(self, request: Request) -> tuple[str, float]:
        seq = await self._redis.incr(self.seq_key)
        # The sequence number is part of the member so identical payloads
        # (e.g. a forced retry) never collapse into one sorted-set entry.
        member = f"{seq}|{json.dumps(request.to_dict(), separators=(',', ':'))}"
        return member, self._score(request.priority, seq)

    async def push(self, request: Request) -> None:
        member, score = await self._entry(request)
        await self._redis.zadd(self.queue_key, {member: score})

    @staticmethod
    def _decode(member: bytes | str) -> Request:
        _, payload = _text(member).split("|", 1)
        return Request.from_dict(json.loads(payload))

    async def pop(self) -> Request | None:
        """Pop without a lease: the entry is gone for good. The engine uses ``receive``."""
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

    # ------------------------------------------------------------------ leases

    async def receive(self, lease_ttl: float) -> Delivery | None:
        lease = uuid.uuid4().hex
        result = await self._receive(keys=self._keys, args=[self._now() + lease_ttl, lease])
        if not result:
            return None
        member, deliveries = result
        return Delivery(self._decode(member), lease=lease, deliveries=int(deliveries))

    async def ack(self, lease: str) -> bool:
        return bool(await self._ack(keys=self._keys, args=[lease]))

    async def requeue(self, lease: str | None, request: Request) -> bool:
        if lease is None:
            await self.push(request)
            return True
        member, score = await self._entry(request)
        return bool(await self._requeue(keys=self._keys, args=[lease, member, score]))

    async def release(self, leases: Iterable[str]) -> int:
        leases = list(leases)
        if not leases:
            return 0
        return int(await self._release(keys=self._keys, args=leases))

    async def extend(self, leases: Iterable[str], lease_ttl: float) -> list[str]:
        leases = list(leases)
        if not leases:
            return []
        lost = await self._extend(keys=[self.leases_key], args=[self._now() + lease_ttl, *leases])
        return [_text(lease) for lease in lost]

    async def reclaim(self) -> int:
        total = 0
        while True:
            seen, reclaimed = await self._reclaim(
                keys=self._keys, args=[self._now(), _RECLAIM_BATCH]
            )
            total += int(reclaimed)
            if int(seen) < _RECLAIM_BATCH:
                return total

    async def in_flight(self) -> int:
        return int(await self._redis.zcard(self.leases_key))

    # ------------------------------------------------------------------ state

    async def snapshot(self) -> FrontierSnapshot:
        """Queued and leased (in flight in any process) requests, plus the seen set."""
        queued = await self._redis.zrange(self.queue_key, 0, -1, withscores=True)
        entries = [(float(score), _text(member)) for member, score in queued]
        for raw in (await self._redis.hgetall(self.inflight_key)).values():
            score, member = _text(raw).split("|", 1)
            entries.append((float(score), member))
        entries.sort()
        seen = [_text(fp) async for fp in self._redis.sscan_iter(self.seen_key, count=1000)]
        return {
            "pending": [self._decode(member).to_dict() for _, member in entries],
            "seen": sorted(seen),
        }

    async def clear(self) -> None:
        await self._redis.delete(
            self.queue_key,
            self.seen_key,
            self.seq_key,
            self.leases_key,
            self.inflight_key,
            self.deliveries_key,
        )

    async def close(self) -> None:
        await self._redis.aclose()
