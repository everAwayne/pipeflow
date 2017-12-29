import time
import redis
import pika
import aio_pika
import asyncio
from ..tasks import Task
from ..clients import RedisMQClient
from ..log import logger


__all__ = ['AbstractInputEndpoint', 'AbstractOutputEndpoint',
           'AbstractCoroutineInputEndpoint', 'AbstractCoroutineOutputEndpoint',
           'QueueInputEndpoint', 'QueueOutputEndpoint',
           'RedisInputEndpoint', 'RedisOutputEndpoint']


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


class RedisInputEndpoint(RedisMQClient, AbstractInputEndpoint):
    """Redis input endpoint"""

    def __init__(self, queue_name, **conf):
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        super(RedisInputEndpoint, self).__init__(**conf)

    def get(self):
        msg = self._get(self._queue_name, 20)
        task = Task(msg)
        return task

    def _get(self, queue_name, time_out):
        """Get a message from a list
        """
        while True:
            try:
                data = self._client.brpop(queue_name, time_out)
                if data is None:
                    continue
                _, raw_data = data
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                self._client.connection_pool.disconnect()
                time.sleep(5)
                continue
                #raise error.MQClientConnectionError()
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()
                continue
                #raise error.MQClientTimeoutError()
            else:
                return raw_data


class RedisOutputEndpoint(RedisMQClient, AbstractOutputEndpoint):
    """Redis output endpoint"""

    def __init__(self, queue_names, direction="left", **conf):
        if not isinstance(queue_names, (str, list)):
            raise ValueError("queue_names must be a string or a list")
        if direction not in ["left", "right"]:
            raise ValueError("invalid direction")
        self._queue_name_ls = [queue_names] if isinstance(queue_names, str) else queue_names
        self._direction = direction
        super(RedisOutputEndpoint, self).__init__(**conf)

    def put(self, tasks):
        msg_dct = {}
        for queue_name, task in tasks:
            queue_name = queue_name or self._queue_name_ls[0]
            task_ls = msg_dct.setdefault(queue_name, [])
            task_ls.append(task)
        for queue_name in msg_dct:
            self._put(queue_name, msg_dct[queue_name])
        return True

    def _put(self, queue_name, tasks):
        """Put a message into a list

        Use lpush
        """
        while True:
            try:
                if self._direction == "left":
                    self._client.lpush(queue_name, *[task.get_raw_data() for task in tasks])
                else:
                    self._client.rpush(queue_name, *[task.get_raw_data() for task in tasks])
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                self._client.connection_pool.disconnect()
                time.sleep(5)
                continue
                #raise error.MQClientConnectionError()
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()
                continue
                #raise error.MQClientTimeoutError()
            else:
                return True
