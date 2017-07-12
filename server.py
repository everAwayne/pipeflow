import functools
import asyncio
import concurrent.futures
from . import endpoints
from . import tasks
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
        self._endpoint_map = {}
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
        self._add_endpoint(name)
        self._loop.create_task(self.fetch_task(name, input_endpoint))
        self._input_endpoint_cnt += 1

    def add_output_endpoint(self, name, output_endpoint, buffer_size=None):
        """Bind an output endpoints to server
        """
        assert endpoints.isoutputendpoint(output_endpoint), "is not outputendpoint"
        self._add_endpoint(name)
        self._result_q_map[name] = asyncio.Queue(
                buffer_size if buffer_size else self._max_concurrency, loop=self._loop)
        if len(self._result_q_map) == 1:
            self._default_result_q = self._result_q_map[name]
        self._loop.create_task(self.send_result(name, self._result_q_map[name], output_endpoint))
        self._output_endpoint_cnt += 1

    def _add_endpoint(self, name):
        assert name not in self._endpoint_map, "endpoint '%s' already exist" % (name,)
        self._endpoint_map[name] = None

    def suspend_endpoint(self, name):
        """Suspend corresponding endpoint
        """
        assert name in self._endpoint_map, "corresponding endpoint isn't bound to the server"
        if self._endpoint_map.get(name) is None:
            self._endpoint_map[name] = asyncio.Event()
        self._endpoint_map[name].clear()

    def resume_endpoint(self, name):
        """Resume corresponding endpoint
        """
        assert name in self._endpoint_map, "corresponding endpoint isn't bound to the server"
        if self._endpoint_map.get(name) is not None:
            self._endpoint_map[name].set()
            self._endpoint_map[name] = None

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
            except:
                self._running_cnt -= 1
                raise
            else:
                self._running_cnt -= 1
                if task is not None:
                    if isinstance(task, tasks.Task):
                        result_q = task.get_to()
                        await self._result_q_map.get(result_q, self._default_result_q).put(task)
                    else:
                        for t in task:
                            result_q = t.get_to()
                            await self._result_q_map.get(result_q, self._default_result_q).put(t)
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

    def add_worker(self, worker, *args, **kw):
        self._loop.create_task(worker(self, *args, **kw))

    async def fetch_task(self, name, input_endpoint):
        """Fetch task from input_endpoint, and put into task queue_name.

        If input_endpoint doesn's support coroutine, executor in thread.
        """
        is_coroutine = endpoints.iscoroutineinputendpoint(input_endpoint)
        if not is_coroutine:
            executor = concurrent.futures.ThreadPoolExecutor()
        while True:
            if self._endpoint_map[name] is not None:
                await self._endpoint_map[name].wait()
            if is_coroutine:
                task = await input_endpoint.get()
            else:
                future = self._loop.run_in_executor(executor, input_endpoint.get)
                task = await future
            task._set_from(name)
            await self._finish_q.put(True)
            await self._task_q.put(task)
            self._loop.create_task(self._worker())

    async def send_result(self, name, result_q, output_endpoint):
        """Get task from result queue, and put into output_endpoint.

        If output_endpoint doesn's support coroutine, executor in thread.
        """
        is_coroutine = endpoints.iscoroutineoutputendpoint(output_endpoint)
        if not is_coroutine:
            executor = concurrent.futures.ThreadPoolExecutor()
        while True:
            if self._endpoint_map[name] is not None:
                await self._endpoint_map[name].wait()
            task = await result_q.get()
            if is_coroutine:
                await output_endpoint.put(task)
            else:
                future = self._loop.run_in_executor(executor, output_endpoint.put, task)
                await future

    def run(self):
        """Start the server
        """
        self._check_endpoint()
        self._check_worker()
        self._loop.run_forever()
