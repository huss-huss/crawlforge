import fakeredis
import pytest

from crawlforge.queues import MemoryQueue
from crawlforge.queues.redis import RedisQueue


@pytest.fixture(params=["memory", "redis"])
async def backend(request):
    if request.param == "memory":
        yield MemoryQueue()
        return
    client = fakeredis.FakeAsyncRedis()
    queue = RedisQueue(client, prefix="test")
    yield queue
    await queue.close()
