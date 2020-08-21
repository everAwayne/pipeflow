# Pipeflow

**Pipeflow** is simple library that you can set up service based on different kinds of MQ.

## Installation

```console
$ python -m pip install pypipeflow
```

Pipeflow only supports Python 3.5+.

## Usage example

Pipeflow offer different type of endpoint
```python
# NSQ endpoint
from pipeflow.endpoints.nsq_endpoints import NsqInputEndpoint, NsqOutputEndpoint

in_endpoint = NsqInputEndpoint("topic1", "channel1", lookupd_http_addresses="127.0.0.1:4161")
output_endpoint = NsqOutputEndpoint(nsqd_tcp_addresses="127.0.0.1:4150")


# Rabbitmq endpoint
from pipeflow.endpoints.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint

RABBITMQ_CONF = {
    "host": "127.0.0.1", "port": 5672, "virtualhost": "/", "login": "login_name",
    "password": "xxxx"
}
in_endpoint = RabbitmqInputEndpoint("queue1", **RABBITMQ_CONF)
output_endpoint = RabbitmqOutputEndpoint("queue1", **RABBITMQ_CONF)


# Redis endpoint
from pipeflow.endpoints.redis_endpoints import RedisInputEndpoint, RedisOutputEndpoint

in_endpoint = RedisInputEndpoint("queue1", host="127.0.0.1", port='6379', password="xxx", db=0)
output_endpoint = RedisOutputEndpoint(host="127.0.0.1", port='6379', password="xxx", db=0)
```

Set up service
```python
from pipeflow.server import Server, Task

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
# NSQ
group.add_output_endpoint('out_name', output_endpoint, topic='topic1', delay=3000)
# Rabbitmq
group.add_output_endpoint('out_name', output_endpoint, queue='queue1')
# Redis
group.add_output_endpoint('out_name', output_endpoint, queue='queue1')

server.run()
```
