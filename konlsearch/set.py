import typing

import rocksdict


class KonlSet:
    def __init__(self, cf: rocksdict.Rdict, prefix: str):
        self._cf = cf
        self._prefix = f'{prefix}:set'

    def __contains__(self, k: str) -> bool:
        key = self.__build_key_name(k)

        return key in self._cf

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def add(self, k: str):
        key = self.__build_key_name(k)

        if key not in self._cf:
            self._cf[key] = "1"

    def remove(self, k: str):
        key = self.__build_key_name(k)

        if key in self._cf:
            self._cf.delete(key)

    def items(self) -> typing.Generator[str, None, None]:
        it = self._cf.iter()
        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            yield self.__remove_prefix(it.key())
            it.next()

    def update(self, s: typing.Set[str]):
        for k in s:
            self.add(k)

    def __build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def __remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")

    def toView(self):
        iter = self._cf.iter()

        return KonlSetView(iter, self._prefix)


class KonlSetView:
    def __init__(self, iter: rocksdict.RdictIter, prefix: str):
        self._iter = iter
        self._prefix = f'{prefix}:set'

    def __contains__(self, k: str) -> bool:
        key = self.__build_key_name(k)

        self._iter.seek(key)

        return self._iter.valid() and self._iter.key() == key

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def items(self) -> typing.Generator[str, None, None]:
        it = self._iter
        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            yield self.__remove_prefix(it.key())
            it.next()

    def __build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def __remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")


class KonlSetWriteBatch:
    def __init__(self, wb: rocksdict.WriteBatch, prefix: str):
        self._wb = wb
        self._prefix = f'{prefix}:set'

    def add(self, k: str):
        key = self.__build_key_name(k)

        self._wb[key] = "1"

    def remove(self, k: str):
        key = self.__build_key_name(k)

        del self._wb[key]

    def update(self, s: typing.Set[str]):
        for k in s:
            self.add(k)

    def __build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'
