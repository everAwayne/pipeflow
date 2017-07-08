__all__ = ['Task']

class Task:
    def __init__(self, data=None):
        assert isinstance(data, bytes), "task data must be bytes"
        self._data = data

    def set_data(self, data):
        assert isinstance(data, bytes), "task data must be bytes"
        self._data = data

    def get_data(self):
        return self._data
