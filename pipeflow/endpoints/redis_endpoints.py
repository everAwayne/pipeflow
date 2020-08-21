import time
import redis
import socket
from ..tasks import Task
from ..log import logger
from .endpoints import AbstractInputEndpoint, AbstractOutputEndpoint


__all__ = ['RedisInputEndpoint', 'RedisOutputEndpoint']


class RedisMQClient:
    """Redis message queue client"""

    _keepalive_opt = {}

    def __init__(self, **conf):

        if hasattr(socket, "TCP_KEEPIDLE"):
            self._keepalive_opt[socket.TCP_KEEPIDLE] = 60
        if hasattr(socket, "TCP_KEEPCNT"):
            self._keepalive_opt[socket.TCP_KEEPCNT] = 3
        if hasattr(socket, "TCP_KEEPINTVL"):
            self._keepalive_opt[socket.TCP_KEEPINTVL] = 60

        _conf = {
            "socket_timeout": 30,
            "socket_connect_timeout": 15,
            "socket_keepalive": True,
            "socket_keepalive_options": self._keepalive_opt
        }
        _conf.update(conf)
        self._client = redis.Redis(**_conf)


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
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()
                continue
            else:
                return raw_data


class RedisOutputEndpoint(RedisMQClient, AbstractOutputEndpoint):
    """Redis output endpoint"""

    def __init__(self, direction="left", **conf):
        #if not isinstance(queue_names, (str, list)):
        #    raise ValueError("queue_names must be a string or a list")
        if direction not in ["left", "right"]:
            raise ValueError("invalid direction")
        #self._queue_name_ls = [queue_names] if isinstance(queue_names, str) else queue_names
        self._direction = direction
        super(RedisOutputEndpoint, self).__init__(**conf)

    def put(self, tasks):
        msg_dct = {}
        for params, task in tasks:
            queue_name = params.get("queue")
            if queue_name is None:
                continue
            #queue_name = queue_name or self._queue_name_ls[0]
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
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                self._client.connection_pool.disconnect()
                continue
            else:
                return True
