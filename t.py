from pipeflow.server import Server
from pipeflow.tasks import Task

'''
from pipeflow.endpoints.nsq_endpoints import NsqInputEndpoint, NsqOutputEndpoint
in_endpoint = NsqInputEndpoint("topic1", 'channel1',
                               lookupd_http_addresses="170.106.152.94:25761")
output_endpoint = NsqOutputEndpoint(nsqd_tcp_addresses="170.106.152.94:25750")
'''

'''
from pipeflow.endpoints.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
RABBITMQ_CONF = {
    'host': '103.92.210.114',
    'port': 10584,
    'virtualhost': "/",
    'heartbeat_interval': 0,
    'login': 'linkcool',
    'password': 'SybfwOwWhchPMgnD'
}
in_endpoint = RabbitmqInputEndpoint('queue1', **RABBITMQ_CONF)
output_endpoint = RabbitmqOutputEndpoint('queue1', **RABBITMQ_CONF)
'''

#'''
from pipeflow.endpoints.redis_endpoints import RedisInputEndpoint, RedisOutputEndpoint
in_endpoint = RedisInputEndpoint("queue1", host="104.217.128.26", port='12111',
                                 password="33fa5bf446e431cf263cc29c8eec0fbb", db=0)
output_endpoint = RedisOutputEndpoint(host="104.217.128.26", port='12111',
                                      password="33fa5bf446e431cf263cc29c8eec0fbb", db=0)
#'''


async def worker_handle(group, task):
    data = task.get_data()
    data += b"--"
    res_task = Task(data)
    res_task.set_to('out_name')
    return res_task

server = Server()
group = server.add_group('group1', concurrency=2)
group.set_handle(worker_handle)
group.add_input_endpoint('in_name', in_endpoint)

#group.add_output_endpoint('out_name', output_endpoint, topic='topic1', delay=3000)
#group.add_output_endpoint('out_name', output_endpoint, queue='queue1')
#group.add_output_endpoint('out_name', output_endpoint, queue='queue1')

server.run()
