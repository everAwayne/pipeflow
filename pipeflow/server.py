import functools
import asyncio
import concurrent.futures
from datetime import datetime
from . import endpoints
from . import tasks
from .log import logger
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


__all__ = ['Server']


class Server:
    """
    """
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._group_map = {}

    def get_event_loop(self):
        return self._loop

    def add_group(self, name, concurrency, task_queue_size=None):
        """
        """
        assert name not in self._group_map, "group '%s' already exist" % name
        if concurrency <= 0:
            raise ValueError("concurrency must be greater than 0")
        self._group_map[name] = Group(concurrency, self._loop, task_queue_size or concurrency)
        return self._group_map[name]

    def add_worker(self, worker, *args, **kw):
        self._loop.create_task(worker(self, *args, **kw))

    def add_routine_worker(self, worker, interval=None, immediately=False, day=None,
                           weekday=None, hour=None, minute=None, *args, **kw):
        """
        unit of interval is minute
        day range: 1-31
        weekday range: 0-6
        hour range: 0-23
        minute range: 0-59
        """
        run_type = None
        if interval:
            run_type = 'interval'
            interval = int(interval*60)
        elif any(map(lambda x:x is not None, [day, weekday, hour, minute])):
            run_type = 'crontab'
        else:
            assert ValueError("must select one of modes: interval, crontab")

        @functools.wraps(worker)
        async def wraper():
            while True:
                if run_type == 'interval':
                    if not immediately:
                        await asyncio.sleep(interval)
                    try:
                        await worker(*args, **kw)
                    except Exception as exc:
                        logger.error("Routine worker error", exc_info=(type(exc), exc, exc.__traceback__))
                        exc.__traceback__ = None
                    if immediately:
                        await asyncio.sleep(interval)
                else:
                    if not immediately:
                        await asyncio.sleep(60)
                    time_now = datetime.now()
                    match = True
                    for _, v in zip(('day', 'weekday', 'hour', 'minute'), (day, weekday, hour, minute)):
                        if v is not None:
                            if _ == 'day' and time_now.day != v:
                                match = False
                                break
                            if _ == 'weekday' and time_now.weekday() != v:
                                match = False
                                break
                            if _ == 'hour' and time_now.hour != v:
                                match = False
                                break
                            if _ == 'minute' and time_now.minute != v:
                                match = False
                                break
                    if match:
                        try:
                            await worker(*args, **kw)
                        except Exception as exc:
                            logger.error("Routine worker error", exc_info=(type(exc), exc, exc.__traceback__))
                            exc.__traceback__ = None
                    if immediately:
                        await asyncio.sleep(60)
        self._loop.create_task(wraper())

    def run(self):
        """Start the server
        """
        self._loop.run_forever()


class Group:
    """
    """
    def __init__(self, concurrency, loop, task_queue_size):
        self._concurrency = concurrency
        self._loop = loop
        self._running_cnt = 0
        self._task_q = asyncio.Queue(task_queue_size, loop=self._loop)
        self._result_q_map = {}
        self._output_name_map = {}
        self._endpoint_map = {}
        self._worker_executor = None

    def get_running_cnt(self):
        return self._running_cnt

    def add_input_endpoint(self, name, input_endpoint):
        """Bind an input endpoints to group
        """
        assert endpoints.isinputendpoint(input_endpoint), "is not inputendpoint"
        self._add_endpoint(name)
        self._loop.create_task(self.fetch_task(name, input_endpoint))

    def add_output_endpoint(self, name, output_endpoint, buffer_size=None, **kw):
        """Bind an output endpoints to group
        """
        assert endpoints.isoutputendpoint(output_endpoint), "is not outputendpoint"
        self._add_endpoint(name)
        if output_endpoint not in self._result_q_map:
            result_q = asyncio.Queue(buffer_size if buffer_size else self._concurrency,
                                      loop=self._loop)
            self._result_q_map[output_endpoint] = result_q
            self._loop.create_task(self.send_result(result_q, output_endpoint))
        self._output_name_map[name] = (kw, self._result_q_map[output_endpoint])

    def _add_endpoint(self, name):
        assert name not in self._endpoint_map, "endpoint '%s' already exist" % name
        self._endpoint_map[name] = None

    def suspend_endpoint(self, name):
        """Suspend corresponding endpoint
        """
        assert name in self._endpoint_map, "corresponding endpoint isn't bound to the group"
        if self._endpoint_map.get(name) is None:
            self._endpoint_map[name] = asyncio.Event()
        self._endpoint_map[name].clear()

    def resume_endpoint(self, name):
        """Resume corresponding endpoint
        """
        assert name in self._endpoint_map, "corresponding endpoint isn't bound to the group"
        if self._endpoint_map.get(name) is not None:
            self._endpoint_map[name].set()
            self._endpoint_map[name] = None

    async def async_return_task(self, task):
        if isinstance(task, tasks.Task):
            name = task.get_to()
            params, result_q = self._output_name_map[name]
            await result_q.put((params, task))
        else:
            for t in task:
                name = t.get_to()
                params, result_q = self._output_name_map[name]
                await result_q.put((params, t))

    def _create_work(self, func, run_type="coroutine"):
        """ Wrap func into coroutine, func will run as a coroutine
        """
        @functools.wraps(func)
        async def worker():
            if run_type=='coroutine':
                coro = asyncio.coroutines.coroutine(func)
            while True:
                task = await self._task_q.get()
                try:
                    self._running_cnt += 1
                    if run_type=='coroutine':
                        task = await coro(self, task)
                    else:
                        task = await self._loop.run_in_executor(self._worker_executor,
                                                                func, self, task)
                except Exception as exc:
                    self._running_cnt -= 1
                    exc_info = (type(exc), exc, exc.__traceback__)
                    logger.error("Error occur in handle", exc_info=exc_info)
                    exc.__traceback__ = None
                else:
                    self._running_cnt -= 1
                    if task is not None:
                        if isinstance(task, tasks.Task):
                            name = task.get_to()
                            params, result_q = self._output_name_map[name]
                            await result_q.put((params, task))
                        else:
                            for t in task:
                                name = t.get_to()
                                params, result_q = self._output_name_map[name]
                                await result_q.put((params, t))
        return worker

    def set_handle(self, handle, run_type="coroutine"):
        """Setting worker's handle of group

        For each task, group will create workers with the handle.
        Handle can run in three different modles: coroutine, thread, process,
        depending on implement detail.
        """
        if run_type == "coroutine":
            pass
        elif run_type == "thread":
            if self._worker_executor is None:
                self._worker_executor = concurrent.futures.ThreadPoolExecutor(self._concurrency)
        elif run_type == "process":
            if self._worker_executor is None:
                self._worker_executor = concurrent.futures.ProcessPoolExecutor(self._concurrency)
        else:
            raise ValueError("the value of run_type is not supported")
        for _ in range(self._concurrency):
            worker = self._create_work(handle, run_type)
            self._loop.create_task(worker())

    async def fetch_task(self, name, input_endpoint):
        """Fetch task from input_endpoint, and put into task queue_name.

        If input_endpoint doesn's support coroutine, executor in thread.
        """
        is_coroutine = endpoints.iscoroutineinputendpoint(input_endpoint)
        if not is_coroutine:
            executor = concurrent.futures.ThreadPoolExecutor(1)
        while True:
            if self._endpoint_map[name] is not None:
                await self._endpoint_map[name].wait()
            if is_coroutine:
                task = await input_endpoint.get()
            else:
                future = self._loop.run_in_executor(executor, input_endpoint.get)
                task = await future
            task.set_from(name)
            await self._task_q.put(task)

    async def send_result(self, result_q, output_endpoint):
        """Get task from result queue, and put into output_endpoint.

        If output_endpoint doesn's support coroutine, executor in thread.
        """
        is_coroutine = endpoints.iscoroutineoutputendpoint(output_endpoint)
        if not is_coroutine:
            executor = concurrent.futures.ThreadPoolExecutor(1)
        while True:
            task_ls = []
            task = await result_q.get()
            task_ls.append(task)
            while not result_q.empty():
                task = await result_q.get()
                task_ls.append(task)
            put_ret = False
            if is_coroutine:
                put_ret = await output_endpoint.put(task_ls)
            else:
                future = self._loop.run_in_executor(executor, output_endpoint.put, task_ls)
                put_ret = await future
            if put_ret:
                for _, task in task_ls:
                    ret = task.confirm()
                    if asyncio.iscoroutine(ret):
                        await ret
