"""Redis-backed frontier: per-domain sorted-set queues, a ready-domain index, a fingerprint
set and leases for in-flight work.

Keys (``<prefix>`` is ``crawlforge:<spider>`` from the CLI)::

    <prefix>:domains           zset   domain -> index score (see below), domains with work
    <prefix>:domains:<domain>  zset   "<seq>|<domain>|<request json>" -> score
                                      (priority desc, FIFO), one queue per domain
    <prefix>:queued            int    queued entries across all domains
    <prefix>:turn              int    round-robin counter for the domain index
    <prefix>:seen              set    request fingerprints (shared, so no URL is queued twice)
    <prefix>:seq               int    insertion counter
    <prefix>:leases            zset   lease id -> deadline (unix seconds)
    <prefix>:inflight          hash   lease id -> "<queue score>|<queue member>"
    <prefix>:deliveries        hash   queue member -> times it was handed out

A domain's index score is the priority band of its best queued entry plus its turn: the
domain with the best next request is served first, and domains tied on priority take turns,
because a domain that was just served draws a new (higher) turn. ``receive`` walks the index
in that order, skips the domains the caller can't send to right now, and moves the head of
the first other domain into ``leases``/``inflight``, all in one Lua script, so two processes
never get the same entry and the index never disagrees with the queues. A domain is in the
index exactly while its queue is non-empty; a member names its domain, so scripts that put
an entry back (retry, release, reclaim) know which queue it belongs to.

The worker then acks the lease (done or dead-lettered), re-queues a retry in its place, or
releases it on shutdown. A lease that is not extended before its deadline (the process died
or hung) is reclaimed by any process: the entry goes back to its domain queue with its
original score and keeps its delivery count, so an entry that keeps expiring can be
dead-lettered instead of being handed out forever.

The per-domain keys are derived inside the scripts, so this needs a single Redis (no
Cluster). Deadlines come from the clients' wall clocks, so processes sharing a queue need
roughly synchronized clocks (NTP); the lease TTL should dwarf any skew.

Frontiers written by older versions (one ``<prefix>:queue`` sorted set) are moved into the
per-domain queues the first time an instance touches them.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Callable, Collection, Iterable
from typing import Any

from crawlforge.http import Request
from crawlforge.queues.base import Delivery, FrontierSnapshot, QueueBackend

# Scores encode (priority desc, insertion asc) in one float. Doubles are exact
# up to 2**53 (~9e15), so with a 1e12 sequence window priorities in [-1000, 1000]
# stay exact. The domain index uses the same layout with the turn in place of the sequence.
_SEQ_SPACE = 1_000_000_000_000
_MAX_PRIORITY = 1000
_RECLAIM_BATCH = 500
_DELETE_BATCH = 500

# KEYS (all scripts but EXTEND and SNAPSHOT): domains, leases, inflight, deliveries, turn,
# queued. Domain queues are ``KEYS[1] .. ':' .. domain``.
_QUEUE = f"""
local SPACE = {_SEQ_SPACE}
local function num(x) return string.format('%.17g', x) end
local function qkey(domain) return KEYS[1] .. ':' .. domain end
-- "<seq>|<domain>|<json>"; entries from before per-domain queues ("<seq>|<json>") share ''.
local function domain_of(member)
  local a = string.find(member, '|', 1, true)
  if string.sub(member, a + 1, a + 1) == '{{' then return '' end
  return string.sub(member, a + 1, string.find(member, '|', a + 1, true) - 1)
end
local function band(score)
  return math.floor(tonumber(score) / SPACE) * SPACE
end
local function index(domain, turn)
  local head = redis.call('ZRANGE', qkey(domain), 0, 0, 'WITHSCORES')[2]
  if head then
    redis.call('ZADD', KEYS[1], num(band(head) + turn), domain)
  else
    redis.call('ZREM', KEYS[1], domain)
  end
end
local function next_turn()
  return redis.call('INCR', KEYS[5]) % SPACE
end
local function enqueue(score, member)
  local domain = domain_of(member)
  redis.call('ZADD', qkey(domain), score, member)
  redis.call('INCR', KEYS[6])
  local current = redis.call('ZSCORE', KEYS[1], domain)
  if current then
    index(domain, tonumber(current) - band(current))  -- keeps its turn
  else
    index(domain, next_turn())
  end
end
-- Pop the head of the best domain not in ``skip``; it goes to the back of its priority band.
local function dequeue(skip)
  local offset = 0
  while true do
    local batch = redis.call('ZRANGE', KEYS[1], offset, offset + 99)
    if #batch == 0 then return nil end
    local stale = false
    for _, domain in ipairs(batch) do
      if not skip[domain] then
        local popped = redis.call('ZPOPMIN', qkey(domain))
        if #popped > 0 then
          redis.call('DECR', KEYS[6])
          index(domain, next_turn())
          return popped[1], popped[2]
        end
        redis.call('ZREM', KEYS[1], domain)  -- indexed without entries: repair, rescan
        stale = true
        break
      end
    end
    if not stale then offset = offset + #batch end
  end
end
local function skipping(from)
  local skip = {{}}
  for i = from, #ARGV do skip[ARGV[i]] = true end
  return skip
end
"""

# ARGV: score, member.
_PUSH = (
    _QUEUE
    + """
enqueue(ARGV[1], ARGV[2])
return 1
"""
)

# ARGV: skipped domains. Returns the member or nil. No lease: the entry is gone for good.
_POP = (
    _QUEUE
    + """
local member = dequeue(skipping(1))
return member or false
"""
)

# ARGV: deadline, lease id, skipped domains. Returns {member, deliveries} or nil when no
# domain outside ``skip`` has queued entries.
_RECEIVE = (
    _QUEUE
    + """
local member, score = dequeue(skipping(3))
if not member then return false end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[2], score .. '|' .. member)
return {member, redis.call('HINCRBY', KEYS[4], member, 1)}
"""
)

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
    _QUEUE
    + _TAKE
    + """
local score, member = take(ARGV[1])
if not score then return 0 end
redis.call('HDEL', KEYS[4], member)
enqueue(ARGV[3], ARGV[2])
return 1
"""
)

# ARGV: lease ids. The entries go back with their score; the delivery is undone.
_RELEASE = (
    _QUEUE
    + _TAKE
    + """
local released = 0
for i = 1, #ARGV do
  local score, member = take(ARGV[i])
  if score then
    enqueue(score, member)
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
    _QUEUE
    + _TAKE
    + """
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local reclaimed = 0
for _, lease in ipairs(expired) do
  local score, member = take(lease)
  if score then
    enqueue(score, member)
    reclaimed = reclaimed + 1
  end
end
return {#expired, reclaimed}
"""
)

# KEYS: domains, inflight. Returns score, member pairs of queued and leased entries.
_SNAPSHOT = """
local out = {}
for _, domain in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local entries = redis.call('ZRANGE', KEYS[1] .. ':' .. domain, 0, -1, 'WITHSCORES')
  for i = 1, #entries, 2 do
    table.insert(out, entries[i + 1])
    table.insert(out, entries[i])
  end
end
for _, entry in ipairs(redis.call('HVALS', KEYS[2])) do
  local cut = string.find(entry, '|', 1, true)
  table.insert(out, string.sub(entry, 1, cut - 1))
  table.insert(out, string.sub(entry, cut + 1))
end
return out
"""

# KEYS: the usual six, then the old single queue. ARGV: (old member, new member, score)
# triples. Moves each entry (and its delivery count) unless another process already did.
_MIGRATE = (
    _QUEUE
    + """
local moved = 0
for i = 1, #ARGV, 3 do
  if redis.call('ZREM', KEYS[7], ARGV[i]) == 1 then
    enqueue(ARGV[i + 2], ARGV[i + 1])
    local count = redis.call('HGET', KEYS[4], ARGV[i])
    if count then
      redis.call('HDEL', KEYS[4], ARGV[i])
      redis.call('HSET', KEYS[4], ARGV[i + 1], count)
    end
    moved = moved + 1
  end
end
return moved
"""
)


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _split(member: str) -> tuple[str, str | None, str]:
    """``(seq, domain, json)``; ``domain`` is None for members of the old single queue."""
    seq, rest = member.split("|", 1)
    if rest.startswith("{"):
        return seq, None, rest
    domain, payload = rest.split("|", 1)
    return seq, domain, payload


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
        self.domains_key = f"{prefix}:domains"
        self.queued_key = f"{prefix}:queued"
        self.turn_key = f"{prefix}:turn"
        self.seen_key = f"{prefix}:seen"
        self.seq_key = f"{prefix}:seq"
        self.leases_key = f"{prefix}:leases"
        self.inflight_key = f"{prefix}:inflight"
        self.deliveries_key = f"{prefix}:deliveries"
        # The single sorted set of older versions, migrated on first use.
        self.legacy_queue_key = f"{prefix}:queue"
        self._migrated = False
        # Wall clock for lease deadlines; injectable so tests can expire leases.
        self._now = clock
        self._keys = [
            self.domains_key,
            self.leases_key,
            self.inflight_key,
            self.deliveries_key,
            self.turn_key,
            self.queued_key,
        ]
        self._push = client.register_script(_PUSH)
        self._pop = client.register_script(_POP)
        self._receive = client.register_script(_RECEIVE)
        self._ack = client.register_script(_ACK)
        self._requeue = client.register_script(_REQUEUE)
        self._release = client.register_script(_RELEASE)
        self._extend = client.register_script(_EXTEND)
        self._reclaim = client.register_script(_RECLAIM)
        self._snapshot = client.register_script(_SNAPSHOT)
        self._migrate = client.register_script(_MIGRATE)

    @classmethod
    def from_url(cls, url: str, prefix: str = "crawlforge") -> RedisQueue:
        from redis.asyncio import Redis

        return cls(Redis.from_url(url), prefix=prefix)

    def domain_key(self, domain: str) -> str:
        return f"{self.domains_key}:{domain}"

    def _score(self, priority: int, seq: int) -> float:
        priority = max(-_MAX_PRIORITY, min(_MAX_PRIORITY, priority))
        return float(-priority * _SEQ_SPACE + (seq % _SEQ_SPACE))

    async def _entry(self, request: Request) -> tuple[str, float]:
        seq = await self._redis.incr(self.seq_key)
        # The sequence number is part of the member so identical payloads
        # (e.g. a forced retry) never collapse into one sorted-set entry; the domain names
        # the queue the entry goes back to after a lease.
        payload = json.dumps(request.to_dict(), separators=(",", ":"))
        member = f"{seq}|{request.domain}|{payload}"
        return member, self._score(request.priority, seq)

    @staticmethod
    def _decode(member: bytes | str) -> Request:
        return Request.from_dict(json.loads(_split(_text(member))[2]))

    async def _upgrade(self) -> None:
        """Move entries of an old single-queue frontier into the per-domain queues."""
        if self._migrated:
            return
        while batch := await self._redis.zrange(
            self.legacy_queue_key, 0, _RECLAIM_BATCH - 1, withscores=True
        ):
            args: list[Any] = []
            for raw, score in batch:
                member = _text(raw)
                seq, _, payload = _split(member)
                domain = Request.from_dict(json.loads(payload)).domain
                args += [member, f"{seq}|{domain}|{payload}", repr(float(score))]
            await self._migrate(keys=[*self._keys, self.legacy_queue_key], args=args)
        self._migrated = True

    async def push(self, request: Request) -> None:
        await self._upgrade()
        member, score = await self._entry(request)
        await self._push(keys=self._keys, args=[score, member])

    async def pop(self, skip: Collection[str] = ()) -> Request | None:
        """Pop without a lease: the entry is gone for good. The engine uses ``receive``."""
        await self._upgrade()
        member = await self._pop(keys=self._keys, args=list(skip))
        return None if member is None else self._decode(member)

    async def queued_domains(self, domains: Iterable[str]) -> set[str]:
        await self._upgrade()
        domains = list(domains)
        if not domains:
            return set()
        scores = await self._redis.zmscore(self.domains_key, domains)
        return {d for d, score in zip(domains, scores, strict=True) if score is not None}

    async def size(self) -> int:
        await self._upgrade()
        return int(await self._redis.get(self.queued_key) or 0)

    async def add_seen(self, fp: str) -> bool:
        return bool(await self._redis.sadd(self.seen_key, fp))

    async def seen_count(self) -> int:
        return int(await self._redis.scard(self.seen_key))

    # ------------------------------------------------------------------ leases

    async def receive(self, lease_ttl: float, skip: Collection[str] = ()) -> Delivery | None:
        await self._upgrade()
        lease = uuid.uuid4().hex
        result = await self._receive(keys=self._keys, args=[self._now() + lease_ttl, lease, *skip])
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
        await self._upgrade()
        flat = await self._snapshot(keys=[self.domains_key, self.inflight_key])
        entries = sorted((float(flat[i]), _text(flat[i + 1])) for i in range(0, len(flat), 2))
        seen = [_text(fp) async for fp in self._redis.sscan_iter(self.seen_key, count=1000)]
        return {
            "pending": [self._decode(member).to_dict() for _, member in entries],
            "seen": sorted(seen),
        }

    async def clear(self) -> None:
        pattern = re.sub(r"([*?\[\]\\])", r"\\\1", self.domains_key) + ":*"
        queues = [key async for key in self._redis.scan_iter(match=pattern, count=1000)]
        keys = [
            *self._keys,
            self.seen_key,
            self.seq_key,
            self.legacy_queue_key,
            *queues,
        ]
        for i in range(0, len(keys), _DELETE_BATCH):
            await self._redis.delete(*keys[i : i + _DELETE_BATCH])
        self._migrated = True

    async def close(self) -> None:
        await self._redis.aclose()
