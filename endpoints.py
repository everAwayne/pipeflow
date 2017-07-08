import redis
from . import error
from . import tasks
from .clients import RedisMQClient


__all__ = ['RedisInputEndpoint', 'RedisOutputEndpoint']


def isinputendpoint(obj):
    return isinstance(obj, AbstractInputEndpoint)


def isoutputendpoint(obj):
    return isinstance(obj, AbstractOutputEndpoint)


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


class RedisInputEndpoint(RedisMQClient, AbstractInputEndpoint):
    """Redis input endpoint"""

    def __init__(self, queue_name, **conf):
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        super(RedisInputEndpoint, self).__init__(**conf)

    def get(self):
        msg = self._get(self._queue_name, 60)
        task = tasks.Task(msg)
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
                raise error.MQClientConnectionError()
            except redis.TimeoutError as e:
                raise error.MQClientTimeoutError()
            else:
                return raw_data


class RedisOutputEndpoint(RedisMQClient, AbstractOutputEndpoint):
    """Redis output endpoint"""

    def __init__(self, queue_name, **conf):
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        super(RedisOutputEndpoint, self).__init__(**conf)

    def put(self, task):
        msg = task.get_data()
        self._put(self._queue_name, msg)
        return True

    def _put(self, queue_name, msg):
        """Put a message into a list

        Use lpush
        """
        try:
            self._client.lpush(queue_name, msg)
        except redis.ConnectionError as e:
            raise error.MQClientConnectionError()
        except redis.TimeoutError as e:
            raise error.MQClientTimeoutError()
        else:
            return True
