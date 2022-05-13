import collections


class LRUDict(collections.OrderedDict):
    def __init__(self, capacity):
        super().__init__()
        self._capacity = capacity

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def get(self, key, default=None):
        # We have to provide our own implementation to ensure that it delegates to our modified __getitem__().
        if key in self:
            return self.__getitem__(key)
        return None

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.move_to_end(key)
        if len(self) > self._capacity:
            self._remove_lru()

    def _remove_lru(self):
        # We can't use self.popitem() as it calls __getitem__().
        del self[next(iter(self))]
