import nsq
import asyncio
import functools
from collections import defaultdict
from .endpoints import AbstractCoroutineInputEndpoint, AbstractCoroutineOutputEndpoint
from ..tasks import Task, MetaTask
from ..log import logger


__all__ = ['NsqInputEndpoint', 'NsqOutputEndpoint']


class NsqInputEndpoint(AbstractCoroutineInputEndpoint):
    """NSQ input endpoint

    input_end = NsqInputEndpoint('topic_x', 'channel_x', 3,
                                **{'lookupd_http_addresses': ['127.0.0.1:5761']})
    """

    def __init__(self, topic, channel, max_in_flight=5, auto_confirm=True, task_cls=Task,  **conf):
        self.task_cls = task_cls
        self._auto_confirm = auto_confirm
        conf['max_in_flight'] = max_in_flight
        self._inner_q = asyncio.Queue(0)
        self._nsq_reader = nsq.Reader(topic=topic, channel=channel,
                                      message_handler=self._message_handler, **conf)

    def _message_handler(self, message):
        message.enable_async()
        self._inner_q.put_nowait(message)

    async def get(self):
        message = await self._inner_q.get()
        task = self.task_cls(message.body)
        if self._auto_confirm:
            message.finish()
        else:
            task.set_confirm_handle(functools.partial(self._confirm, message))
        return task

    def _confirm(self, message):
        if not message._has_responded:
            message.finish()


class NsqOutputEndpoint(AbstractCoroutineOutputEndpoint):
    """NSQ output endpoint

    output_end = NsqOutputEndpoint(**{'nsqd_tcp_addresses': '127.0.0.1:5750'})
    """

    def __init__(self, **conf):
        self._nsq_writer = nsq.Writer(**conf)

    def _callback(self, conn, data, fut):
        success = True
        if isinstance(data, nsq.Error):
            logger.error(data)
            success = False
        fut.set_result(success)

    async def _mpub(self, topic, msgs):
        fut = asyncio.Future()
        callback = functools.partial(self._callback, fut=fut)
        self._nsq_writer.mpub(topic, msgs, callback)
        return fut

    async def _dpub(self, topic, delay, msg):
        fut = asyncio.Future()
        callback = functools.partial(self._callback, fut=fut)
        self._nsq_writer.dpub(topic, delay, msg, callback)
        return fut

    async def put(self, tasks):
        grp = defaultdict(list)
        for params, task in tasks:
            grp[(params.get("topic"), params.get("delay"))].append(task)
        ret_ls = []
        for params, tasks in grp.items():
            if params[1] is None:
                ret = await self._mpub(params[0], [task.get_raw_data() for task in tasks])
                ret_ls.append(ret)
            elif isinstance(params[1], int):
                for task in tasks:
                    ret = await self._dpub(params[0], params[1], task.get_raw_data())
                    ret_ls.append(ret)
            else:
                logger.error("NsqOutput error: invalid params: {}".format(params))
        return all(ret_ls)


class NsqDynamicOutputEndpoint(NsqOutputEndpoint):
    """NSQ dynamic output endpoint

    use meta data in task to determine topics to pub, and update meta data.

    output_end = NsqDynamicOutputEndpoint(**{'nsqd_tcp_addresses': '127.0.0.1:5750'})
    """

    async def put(self, tasks):
        m_grp = defaultdict(list)
        d_grp = defaultdict(list)
        for _, task in tasks:
            assert isinstance(task, MetaTask)
            for task in task.next_tasks:
                hop = task.current_hop
                if hop.get("delay"):
                    d_grp[hop["topic"], hop["delay"]].append(task)
                else:
                    m_grp[hop["topic"]].append(task)
        ret_ls = []
        for topic, tasks in m_grp.items():
            ret = await self._mpub(topic, [task.get_raw_data() for task in tasks])
            ret_ls.append(ret)
        for params, tasks in d_grp.items():
            for task in tasks:
                ret = await self._dpub(params[0], params[1], task.get_raw_data())
                ret_ls.append(ret)
        return all(ret_ls)
