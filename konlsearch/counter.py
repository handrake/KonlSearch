import typing

import rocksdict

from .dict import KonlDefaultDict
from .set import KonlSet


_DIGIT_COUNT = 8


class KonlCounter:
    def __init__(self, cf: rocksdict.Rdict, prefix: str, max_size: int):
        self._cf = cf
        self._prefix = f'{prefix}:counter'
        self._sorted_set = KonlSet(self._cf, self._prefix)
        self._dict = KonlDefaultDict(self._cf, self._prefix, 0)
        self._max_size = max_size

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def __delitem__(self, key: str):
        if key not in self._dict:
            return

        self._sorted_set.remove(self.build_set_key(key, self._dict[key]))
        del self._dict[key]

    def __getitem__(self, key: str):
        return self._dict[key]

    def increase(self, key: str, increment: int):
        count = self._dict[key]
        new_count = count + increment

        self._dict[key] = new_count

        self._sorted_set.add(self.build_set_key(key, new_count))
        self._sorted_set.remove(self.build_set_key(key, count))

        self.compact()

    def decrease(self, key: str, decrement: int):
        count = self._dict[key]

        if count == 0:
            return

        new_count = count - decrement

        if new_count > 0:
            self._dict[key] = new_count
            self._sorted_set.add(self.build_set_key(key, new_count))

        self._sorted_set.remove(self.build_set_key(key, count))

        self.compact()

    def compact(self):
        if len(self._dict) <= self._max_size:
            return

        last_element = list(self._sorted_set.items())[-1]
        key, value = self.get_from_element(last_element)

        del self._dict[key]
        self._sorted_set.remove(last_element)

    def items(self) -> typing.Generator[tuple[str, int], None, None]:
        for element in self._sorted_set.items():
            yield self.get_from_element(element)

    def build_set_key(self, key: str, count: int) -> str:
        flipped_count = count ^ (16 ** _DIGIT_COUNT - 1)
        count_x = f'{flipped_count:x}'.rjust(_DIGIT_COUNT, '0')
        return f'{count_x}:{key}'

    def get_from_element(self, element: str) -> tuple[str, int]:
        value, key = element.split(":")
        return key, int(value, 16) ^ (16 ** _DIGIT_COUNT - 1)
