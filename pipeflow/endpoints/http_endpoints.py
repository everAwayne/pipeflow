import asyncio
import json
import aiohttp
import yarl
from ..log import logger
from ..tasks import Task
from .endpoints import AbstractCoroutineInputEndpoint, AbstractCoroutineOutputEndpoint


__all__ = ['HTTPInputEndpoint', 'HTTPOutputEndpoint']


class HTTPClient:

    def __init__(self, **conf):
        self._url = conf["url"]
        self._method = conf["method"]
        self._timeout = conf["time_out"]
        self._interval = conf.get('interval', 10)
        if conf.get("params") is not None:
            self._url = str(yarl.URL(self._url).with_query(conf["params"]))
        self._data = conf.get("data")
        self._ignored_ls = conf.get("ignore", [b'{}'])


class HTTPInputEndpoint(HTTPClient, AbstractCoroutineInputEndpoint):

    def __init__(self, **conf):
        super(HTTPInputEndpoint, self).__init__(**conf)

    async def get(self):
        msg = await self._get()
        task = Task(msg)
        return task

    async def _fetch(self):
        async with aiohttp.ClientSession(conn_timeout=self._timeout,
                                         raise_for_status=True) as session:
            async with session.request(self._method, self._url, data=self._data,
                                       timeout=self._timeout) as response:
                return await response.read()

    async def _get(self):
        while True:
            try:
                data = await self._fetch()
            except Exception as e:
                exc_info = (type(e), e, e.__traceback__)
                logger.error('HttpInput error', exc_info=exc_info)
                await asyncio.sleep(self._interval)
                continue
            else:
                if data in self._ignored_ls:
                    await asyncio.sleep(self._interval)
                    continue
                await asyncio.sleep(1)
                return data


class HTTPOutputEndpoint(HTTPClient, AbstractCoroutineOutputEndpoint):

    def __init__(self, **conf):
        super(HTTPOutputEndpoint, self).__init__(**conf)

    async def put(self, tasks):
        for queue_name, task in tasks:
            await self._put(task)
        return True

    async def _put(self, task):
        while True:
            try:
                async with aiohttp.ClientSession(conn_timeout=self._timeout,
                                                 raise_for_status=True) as session:
                    async with session.request(self._method, self._url,
                                               data=task.get_raw_data(),
                                               timeout=self._timeout) as response:
                        if response.status != 200:
                            logger.info("status:{}, resp:{}".format(response.status,
                                                                await response.text()))
            except Exception as e:
                exc_info = (type(e), e, e.__traceback__)
                logger.error('HttpOutput error', exc_info=exc_info)
                await asyncio.sleep(self._interval)
                continue
            else:
                await asyncio.sleep(1)
                return True
