
__all__ = ['MQClientConnectionError', 'MQClientTimeoutError']


class PipeFlowError(Exception):
    pass


class MQClientConnectionError(PipeFlowError):
    pass


class MQClientTimeoutError(PipeFlowError):
    pass
