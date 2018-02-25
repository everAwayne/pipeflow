import redis
import socket

class RedisMQClient:
    """Redis message queue client"""

    _keepalive_opt = {}

    def __init__(self, **conf):
        #option may not be portable
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
