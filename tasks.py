__all__ = ['Task']

class Task:
    """Manage raw data and flow control

    _from indicate which input endpoint task come from,
    _to control which output endpoint task will go
    _confirm_handle is function to confirm
    """
    __slots__ = ['_from', '_to', '_data', '_confirm_handle']

    def __init__(self, data=None):
        assert isinstance(data, bytes), "task data must be bytes"
        self._from = None
        self._to = None
        self._confirm_handle = None
        self._data = data

    def set_data(self, data):
        assert isinstance(data, bytes), "task data must be bytes"
        self._data = data

    def get_data(self):
        return self._data

    def get_raw_data(self):
        return self._data

    def set_from(self, name):
        self._from = name

    def get_from(self):
        return self._from

    def set_to(self, name):
        self._to = name

    def get_to(self):
        return self._to

    def set_confirm_handle(self, confirm_handle):
        self._confirm_handle = confirm_handle

    def get_confirm_handle(self):
        return self._confirm_handle

    def confirm(self):
        if self._confirm_handle is not None:
            self._confirm_handle()
            self._confirm_handle = None
