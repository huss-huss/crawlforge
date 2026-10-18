from crawlforge.queues.base import FrontierSnapshot, QueueBackend
from crawlforge.queues.memory import MemoryQueue


def queue_from_spec(spec: str, prefix: str = "crawlforge") -> QueueBackend:
    """Build a backend from ``memory`` or a ``redis://`` / ``rediss://`` URL."""
    if spec in ("", "memory"):
        return MemoryQueue()
    if spec.startswith(("redis://", "rediss://", "unix://")):
        from crawlforge.queues.redis import RedisQueue

        return RedisQueue.from_url(spec, prefix=prefix)
    raise ValueError(f"unknown queue backend: {spec!r} (use 'memory' or a redis:// URL)")


__all__ = ["FrontierSnapshot", "MemoryQueue", "QueueBackend", "queue_from_spec"]
