import time
import redis
import pika
import aio_pika
import asyncio
from .endpoints import AbstractCoroutineInputEndpoint, AbstractCoroutineOutputEndpoint,\
    AbstractInputEndpoint, AbstractOutputEndpoint
from ..clients import RedisMQClient
from ..tasks import Task
from ..log import logger


__all__ = ['TimeInputEndpoint', 'TimeOutputEndpoint']


class _test_TimeInputEndpoint(AbstractCoroutineInputEndpoint):
    """Rabbitmq aio input endpoint"""

    def __init__(self, queue_name, resolution=60, loop=None, **conf):
        self._loop = loop or asyncio.get_event_loop()
        self._resolution = resolution
        self._conf = conf
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._current_slice = int(time.time())//self._resolution-1
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                break

    async def next_queue(self):
        while True:
            time_now = time.time()
            limit_slice = int(time_now)//self._resolution-1
            print('slice: ', self._current_slice, limit_slice)
            if self._current_slice <= limit_slice:
                try:
                    self._channel = await self._connection.channel()
                    self._queue = await self._channel.declare_queue(self._queue_name+':'+str(self._current_slice), passive=True)
                except pika.exceptions.ChannelClosed:
                    self._current_slice += 1
                else:
                    return
            else:
                await asyncio.sleep((limit_slice+2)*self._resolution-time_now)

    async def get(self):
        while True:
            try:
                if not hasattr(self, '_queue'):
                    await self.next_queue()
                message = await self._queue.get(no_ack=True, fail=False)
            except pika.exceptions.ChannelClosed:
                await self.next_queue()
            else:
                if message is None:
                    await self._channel.queue_delete(self._queue_name+':'+str(self._current_slice))
                    self._current_slice += 1
                    await self.next_queue()
                else:
                    task = Task(message.body)
                    return task


class _test_TimeOutputEndpoint(AbstractCoroutineOutputEndpoint):
    """Rabbitmq aio output endpoint"""

    def __init__(self, queue_name, interval, resolution=60, loop=None, **conf):
        self._loop = loop or asyncio.get_event_loop()
        self._interval = interval
        self._resolution = resolution
        self._conf = conf
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
                self._channel = await self._connection.channel()
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                break

    async def put(self, tasks):
        for task in tasks:
            message = aio_pika.Message(task.get_raw_data())
            dest_slice = int(time.time()+self._resolution*self._interval)//self._resolution
            queue_name = self._queue_name+':'+str(dest_slice)
            await self._channel.declare_queue(queue_name, durable=True)
            ret = await self._channel.default_exchange.publish(message, routing_key=queue_name)
        return True


class TimeInputEndpoint(RedisMQClient, AbstractInputEndpoint):
    """Redis time input endpoint"""

    def __init__(self, queue_name, resolution=60, **conf):
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._resolution = resolution
        self._current_slice = int(time.time())//self._resolution-1
        super(TimeInputEndpoint, self).__init__(**conf)

    def get(self):
        msg = self._get(self._queue_name)
        task = Task(msg)
        return task

    def _get(self, queue_name):
        """Get a message from a list
        """
        while True:
            try:
                time_now = time.time()
                limit_slice = int(time_now)//self._resolution-1
                if self._current_slice <= limit_slice:
                    data = self._client.rpop(self._queue_name+':'+str(self._current_slice))
                    if data is None:
                        self._current_slice += 1
                    else:
                        return data
                else:
                    time.sleep((limit_slice+2)*self._resolution-time_now)
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                self._client.connection_pool.disconnect()
                time.sleep(5)
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()


class TimeOutputEndpoint(RedisMQClient, AbstractOutputEndpoint):
    """Redis time output endpoint"""

    def __init__(self, queue_names, resolution=60, **conf):
        if not isinstance(queue_names, (tuple, list)):
            raise ValueError("queue_names must be a tuple or a list")
        self._queue_name_ls = [queue_names] if isinstance(queue_names, tuple) else queue_names
        self._resolution = resolution
        super(TimeOutputEndpoint, self).__init__(**conf)

    def put(self, tasks):
        msg_dct = {}
        for queue_name, task in tasks:
            queue_name = queue_name or self._queue_name_ls[0]
            msg_ls = msg_dct.setdefault(queue_name, [])
            msg_ls.append(task.get_raw_data())
        for queue_name in msg_dct:
            self._put(queue_name, msg_dct[queue_name])
        return True

    def _put(self, queue_name, msgs):
        """Put messages into a list

        Use lpush
        """
        while True:
            queue_name, interval = queue_name
            dest_slice = int(time.time()+self._resolution*interval)//self._resolution
            queue_name = queue_name+':'+str(dest_slice)
            try:
                self._client.lpush(queue_name, *msgs)
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                self._client.connection_pool.disconnect()
                time.sleep(5)
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()
            else:
                return True
