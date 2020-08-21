import asyncio
from ..tasks import Task
from ..log import logger


__all__ = ['AbstractInputEndpoint', 'AbstractOutputEndpoint',
           'AbstractCoroutineInputEndpoint', 'AbstractCoroutineOutputEndpoint',
           'QueueInputEndpoint', 'QueueOutputEndpoint']


def isinputendpoint(obj):
    return isinstance(obj, (AbstractInputEndpoint, AbstractCoroutineInputEndpoint))


def isoutputendpoint(obj):
    return isinstance(obj, (AbstractOutputEndpoint, AbstractCoroutineOutputEndpoint))


def iscoroutineinputendpoint(obj):
    return isinstance(obj, AbstractCoroutineInputEndpoint)


def iscoroutineoutputendpoint(obj):
    return isinstance(obj, AbstractCoroutineOutputEndpoint)


class AbstractInputEndpoint:
    """Abstract input endpoint"""

    def get(self):
        """Get a message from queue and parse message into a Task object

        Return a Task object, or raise its exception
        """
        raise NotImplementedError


class AbstractOutputEndpoint:
    """Abstract output endpoint"""

    def put(self, task):
        """Parse Task object into message and put into queue
        """
        raise NotImplementedError


class AbstractCoroutineInputEndpoint:
    """Abstract coroutine input endpoint"""

    async def get(self):
        """Get a message from queue and parse message into a Task object

        Return a Task object, or raise its exception
        """
        raise NotImplementedError


class AbstractCoroutineOutputEndpoint:
    """Abstract coroutine output endpoint"""

    async def put(self, task):
        """Parse Task object into message and put into queue
        """
        raise NotImplementedError


class QueueInputEndpoint(AbstractCoroutineInputEndpoint):
    """Queue input endpoint"""

    def __init__(self, queue):
        if queue is None:
            raise ValueError("queue must be not None")
        assert isinstance(queue, asyncio.Queue), "queue must be an isinstance of asyncio.Queue"
        self._queue = queue

    async def get(self):
        msg = await self._queue.get()
        task = Task(msg)
        return task


class QueueOutputEndpoint(AbstractCoroutineOutputEndpoint):
    """Queue output endpoint"""

    def __init__(self, queue):
        if queue is None:
            raise ValueError("queue must be not None")
        assert isinstance(queue, asyncio.Queue), "queue must be an isinstance of asyncio.Queue"
        self._queue = queue

    async def put(self, tasks):
        for _, task in tasks:
            msg = task.get_raw_data()
            await self._queue.put(msg)
        return True
