
__all__ = ['MQClientConnectionError', 'MQClientTimeoutError']

class WorkFlowError(Exception):
    pass

class MQClientConnectionError(WorkFlowError):
    pass

class MQClientTimeoutError(WorkFlowError):
    pass
