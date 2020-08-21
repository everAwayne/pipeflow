import json
import asyncio

__all__ = ['Task']

class Task:
    """Manage raw data and flow control

    _from indicate which input endpoint task come from,
    _to control which output endpoint task will go
    _confirm_handle is function to confirm
    """
    __slots__ = ['_from', '_to', '_data', '_confirm_handle']

    def __init__(self, data=None, from_name=None, to_name=None, confirm_handle=None):
        if data is not None:
            assert isinstance(data, bytes), "task data must be bytes"
        self._from = from_name
        self._to = to_name
        self._confirm_handle = confirm_handle
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
            ret = self._confirm_handle()
            self._confirm_handle = None
            return ret


class MetaTask(Task):
    """Like Task, additionally, add meta data containing hops conf which determine following
    dynamic ouputenpoints how to publish message.
    """
    __slots__ = Task.__slots__ + ['data', 'next_hop']

    def __init__(self, data=None, from_name=None, to_name=None, confirm_handle=None,
                 next_hop=True):
        super().__init__(data, from_name, to_name, confirm_handle)
        self.data = json.loads(self._data, encoding="utf-8")
        self.next_hop = next_hop

    @property
    def current_hop(self):
        return {
            "topic": self.data["meta"]["hop"]["topic"],
            "delay": self.data["meta"]["hop"].get("delay")
        }

    def set_data(self, data):
        self.data["data"] = data

    def get_data(self):
        return self.data["data"]

    def get_meta_data(self):
        dct = self.data["meta"].copy()
        del dct["hop"]
        return dct

    def spawn(self, data, next_hop=True, hop_conf=None):
        dct = {
            "data": data,
            "meta": self.data["meta"]
        }
        if hop_conf:
            dct["meta"]["hop"] = hop_conf
        return MetaTask(json.dumps(dct).encode("utf-8"), self._from, self._to,
                        self._confirm_handle, next_hop)

    @property
    def next_tasks(self):
        """hop format
        {
            "topic": "name",
            "delay": 5000,    #optional
            "next": [
                {...},
                {...}
            ]
        }
        """
        if self.next_hop:
            dct = {
                "data": self.data["data"],
                "meta": self.data["meta"].copy()
            }
            for hop in self.data["meta"]["hop"].get("next", []):
                dct["meta"]["hop"] = hop
                yield MetaTask(json.dumps(dct).encode("utf-8"))
        else:
            yield self
