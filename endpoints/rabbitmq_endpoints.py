import aio_pika
import asyncio
import functools
from .endpoints import AbstractCoroutineInputEndpoint, AbstractCoroutineOutputEndpoint,\
    AbstractInputEndpoint, AbstractOutputEndpoint
from ..tasks import Task
from ..log import logger


__all__ = ['RabbitmqInputEndpoint', 'RabbitmqOutputEndpoint']


class RabbitMQClient:
    """Rabbitmq client"""

    def __init__(self, **conf):
        #self._conf = {'reconnect_interval': 5}
        self._conf = {}
        self._conf.update(conf)


def ack(message):
    if not message.processed:
        message.ack()


class RabbitmqInputEndpoint(RabbitMQClient, AbstractCoroutineInputEndpoint):
    """Rabbitmq aio input endpoint"""

    def __init__(self, queue_name, loop=None, no_ack=False, qos=1, **conf):
        self._loop = loop
        self._no_ack = no_ack
        self._qos = qos
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._inner_q = asyncio.Queue(1)
        super(RabbitmqInputEndpoint, self).__init__(**conf)
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=self._qos)
                self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                await self._queue.consume(self._callback)
                break

    async def get(self):
        message = await self._inner_q.get()
        task = Task(message.body)
        if self._no_ack:
            task.set_confirm_handle(functools.partial(ack, message))
        return task

    async def _callback(self, message):
        await self._inner_q.put(message)
        if not self._no_ack:
            message.ack()


class RabbitmqOutputEndpoint(RabbitMQClient, AbstractCoroutineOutputEndpoint):
    """Rabbitmq aio output endpoint"""

    def __init__(self, queue_names, persistent=False, loop=None, **conf):
        self._loop = loop
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        if not isinstance(queue_names, (str, list)):
            raise ValueError("queue_names must be a string or a list")
        self._queue_name_ls = [queue_names] if isinstance(queue_names, str) else queue_names
        self._persistent = persistent
        super(RabbitmqOutputEndpoint, self).__init__(**conf)
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
                self._channel = await self._connection.channel()
                for queue_name in self._queue_name_ls:
                    await self._channel.declare_queue(queue_name, durable=True)
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                break

    async def put(self, tasks):
        await self._put(tasks)
        return True

    async def _put(self, tasks):
        """Put a message into a list
        """
        for queue_name, task in tasks:
            queue_name = queue_name or self._queue_name_ls[0]
            if self._persistent:
                message = aio_pika.Message(task.get_raw_data(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
            else:
                message = aio_pika.Message(task.get_raw_data())
            ret = await self._channel.default_exchange.publish(message, routing_key=queue_name)
            if ret:
                task.confirm()
