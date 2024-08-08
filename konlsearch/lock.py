import threading

from typing import Union

type AbcLock = Union[threading.Lock, threading.RLock]
type LockType = Union[type(threading.Lock), type(threading.RLock)]


class StripedLock:
    def __init__(self, lock_type: LockType, stripes: int):
        self._locks = [lock_type() for _ in range(stripes)]
        self.size = stripes

    def __getitem__(self, index: int) -> AbcLock:
        return self._locks[index]

    def get(self, s: str) -> AbcLock:
        return self._locks[hash(s) % self.size]
