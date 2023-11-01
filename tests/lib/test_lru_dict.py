from collections import deque

import hypothesis.strategies as st
import pytest
from hypothesis import assume, settings
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule

from jobrunner.lib.lru_dict import LRUDict


@settings(max_examples=500)
class LRUDictValidation(RuleBasedStateMachine):
    capacity = 5
    keys = st.integers(min_value=1)
    values = st.integers(min_value=1)
    existing_items = Bundle("existing_items")

    def __init__(self):
        super().__init__()
        self.values = dict()
        self.recent_keys = deque(maxlen=self.capacity)
        self.lru_dict = LRUDict(self.capacity)

    @rule(key=keys, value=values, target=existing_items)
    def insert_item_for_the_first_time(self, key, value):
        self._insert(key, value)
        return key, value

    @rule(item=existing_items)
    def reinsert_an_existing_item_unchanged(self, item):
        key, value = item
        self._insert(key, value)

    @rule(item=existing_items, value=values, target=existing_items)
    def overwrite_an_existing_item(self, item, value):
        key, _ = item
        self._insert(key, value)
        return key, value

    @rule(item=existing_items)
    def get_item_thats_present(self, item):
        key, _ = item
        assume(key in self.recent_keys)
        self.lru_dict.get(key)
        self._record_access(key)

    @rule(item=existing_items)
    def get_item_thats_been_evicted(self, item):
        key, _ = item
        assume(key not in self.recent_keys)
        self.lru_dict.get(key)

    @rule(key=keys)
    def get_item_thats_never_been_inserted(self, key):
        assume(key not in self.values)
        self.lru_dict.get(key)

    @invariant()
    def is_the_right_length(self):
        assert len(self.lru_dict) == len(self.recent_keys)

    @invariant()
    def has_a_bounded_length(self):
        assert len(self.lru_dict) <= self.capacity

    @invariant()
    def contains_the_right_items(self):
        assert set(self.lru_dict.keys()) == set(self.recent_keys)

    @invariant()
    def has_the_right_values(self):
        # Note that this operation accesses the values in the LRUDict, which has the side-effect of updating their
        # recency; at first blush this is undesirable because we don't want our invariant checks to modify the data.
        # However in this case it's okay because we touch every item in exactly their recency order (oldest to newest)
        # because were iterating through the recent keys in order; so the entire operation leaves the recency exactly
        # as it was.
        #
        # We have to jigger around getting a static list of the keys because the get implementation for this dict can
        # modify the data, which is illegal during iteration.
        keys = list(self.lru_dict.keys())
        for key in keys:
            assert self.lru_dict[key] == self.values[key]

    def _insert(self, key, value):
        self.lru_dict[key] = value
        self.values[key] = value
        self._record_access(key)

    def _record_access(self, key):
        if key in self.recent_keys:
            self.recent_keys.remove(key)
        self.recent_keys.append(key)


LRUDictTest = pytest.mark.slow_test(LRUDictValidation.TestCase)
