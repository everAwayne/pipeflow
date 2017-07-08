import redis
import socket

class RedisMQClient:
    """Redis message queue client"""

    _keepalive_opt = {
        socket.TCP_KEEPIDLE: 600,
        #socket.TCP_KEEPCNT: ,
        socket.TCP_KEEPINTVL: 60,
    }

    def __init__(self, **conf):
        _conf = {
            #"socket_timeout": 60,
            "socket_connect_timeout": 15,
            "socket_keepalive": True,
            "socket_keepalive_options": self._keepalive_opt
        }
        _conf.update(conf)
        self._client = redis.Redis(**_conf)
