import functools
import asyncio
import concurrent.futures
from . import endpoints
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


__all__ = ['Server']


class Server:
    def __init__(self, max_concurrency=10):
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than 0")

        self._worker = None
        self._max_concurrency = max_concurrency
        self._running_cnt = 0
        self._loop = asyncio.get_event_loop()
        # _finish_q is used for controling worker concurrency
        # Before a task push into _task_q, push a item into _finish_q,
        # After a result push into a _result_q, pop a item from _finish_q
        self._task_q = asyncio.Queue(self._max_concurrency, loop=self._loop)
        self._finish_q = asyncio.Queue(self._max_concurrency, loop=self._loop)
        self._result_q_map = {}
        self._default_result_q = None
        #self._result_q = asyncio.Queue(self._max_concurrency, loop=self._loop)
        self._input_endpoint_cnt = 0
        self._output_endpoint_cnt = 0

    def get_event_loop(self):
        return self._loop

    def get_running_cnt(self):
        return self._running_cnt

    def _check_endpoint(self):
        assert self._input_endpoint_cnt >= 1, "as least must be one input_endpoint"
        assert self._output_endpoint_cnt >= 1, "as least must be one output_endpoint"

    def _check_worker(self):
        assert self._worker is not None, "handle must be set"

    def add_input_endpoint(self, name, input_endpoint):
        """Bind an input endpoints to server
        """
        assert endpoints.isinputendpoint(input_endpoint), "is not inputendpoint"
        self._loop.create_task(self.fetch_task(name, input_endpoint))
        self._input_endpoint_cnt += 1

    def add_output_endpoint(self, name, output_endpoint):
        """Bind an output endpoints to server
        """
        assert endpoints.isoutputendpoint(output_endpoint), "is not outputendpoint"
        assert name not in self._result_q_map, "output_endpoint '%s' already exist" % (name,)
        self._result_q_map[name] = asyncio.Queue(self._max_concurrency, loop=self._loop)
        if len(self._result_q_map) == 1:
            self._default_result_q = self._result_q_map[name]
        self._loop.create_task(self.send_result(self._result_q_map[name], output_endpoint))
        self._output_endpoint_cnt += 1

    def _run_as_coroutine(self, func):
        """ Wrap func into coroutine, func will run as a coroutine
        """
        @functools.wraps(func)
        async def worker():
            coro = asyncio.coroutines.coroutine(func)
            task = await self._task_q.get()
            try:
                self._running_cnt += 1
                task = await coro(self, task)
                self._running_cnt -= 1
            except:
                raise
            else:
                if task is not None:
                    result_q = task.get_to()
                    await self._result_q_map.get(result_q, self._default_result_q).put(task)
            finally:
                await self._finish_q.get()
        return worker

    def _run_as_thread(self, func):
        """ Wrap func into coroutine, func will run as a thread
        """
        @functools.wraps(func)
        async def worker():
            task = await self._task_q.get()
            executor = concurrent.futures.ThreadPoolExecutor()
            future = self._loop.run_in_executor(executor, func, task.get_data())
            try:
                result = await future
            except:
                raise
            else:
                if result is not None:
                    task.set_data(result)
                    await self._result_q.put(task)
            finally:
                await self._finish_q.get()
        return worker

    def _run_as_process(self, func):
        """ Wrap func into coroutine, func will run as a process
        """
        @functools.wraps(func)
        async def worker():
            task = await self._task_q.get()
            executor = concurrent.futures.ProcessPoolExecutor()
            future = self._loop.run_in_executor(executor, func, task.get_data())
            try:
                result = await future
            except:
                raise
            else:
                if result is not None:
                    task.set_data(result)
                    await self._result_q.put(task)
            finally:
                await self._finish_q.get()
        return worker

    def set_handle(self, handle, run_type="coroutine"):
        """Setting worker's handle of server

        For each task, server will create a worker with the handle
        """
        if run_type == "coroutine":
            self._worker = self._run_as_coroutine(handle)
        #elif run_type == "thread":
        #    self._worker = self._run_as_thread(handle)
        #elif run_type == "process":
        #    self._worker = self._run_as_process(handle)
        else:
            raise ValueError("the value of run_type is not supported")

    def add_worker(self, worker):
        self._loop.create_task(worker(self))

    async def fetch_task(self, name, input_endpoint):
        executor = concurrent.futures.ThreadPoolExecutor()
        while True:
            future = self._loop.run_in_executor(executor, input_endpoint.get)
            task = await future
            task._set_from(name)
            await self._finish_q.put(True)
            await self._task_q.put(task)
            self._loop.create_task(self._worker())

    async def send_result(self, result_q, output_endpoint):
        executor = concurrent.futures.ThreadPoolExecutor()
        while True:
            task = await result_q.get()
            future = self._loop.run_in_executor(executor, output_endpoint.put, task)
            await future

    def run(self):
        """Start the server
        """
        self._check_endpoint()
        self._check_worker()
        self._loop.run_forever()
