import abc
import typing

import rocksdict


class AbstractKonlSet(abc.ABC):
    def build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")


class KonlSetReader(AbstractKonlSet):
    @abc.abstractmethod
    def __contains__(self, k: str) -> bool:
        pass

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def items(self) -> typing.Generator[str, None, None]:
        if hasattr(self, "_iter"):
            it = self._iter
        else:
            it = self._cf.iter()
        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            yield self.remove_prefix(it.key())
            it.next()


class KonlSetWriter(AbstractKonlSet):
    @abc.abstractmethod
    def add(self, k: str):
        pass

    @abc.abstractmethod
    def remove(self, k: str):
        pass

    @abc.abstractmethod
    def update(self, s: typing.Set[str]):
        pass


class KonlSetView(KonlSetReader):
    def __init__(self, iter: rocksdict.RdictIter, prefix: str):
        self._iter = iter
        self._prefix = f'{prefix}:set'

    def __contains__(self, k: str) -> bool:
        key = self.build_key_name(k)

        self._iter.seek(key)

        return self._iter.valid() and self._iter.key() == key


class KonlSet(KonlSetReader, KonlSetWriter):
    def __init__(self, cf: rocksdict.Rdict, prefix: str):
        self._cf = cf
        self._prefix = f'{prefix}:set'

    def __contains__(self, k: str) -> bool:
        key = self.build_key_name(k)

        return key in self._cf

    def to_view(self):
        iter = self._cf.iter()

        return KonlSetView(iter, self._prefix)

    def add(self, k: str):
        key = self.build_key_name(k)

        self._cf[key] = "1"

    def remove(self, k: str):
        key = self.build_key_name(k)

        if key in self._cf:
            self._cf.delete(key)

    def update(self, s: typing.Set[str]):
        for k in s:
            self.add(k)


class KonlSetWriteBatch(KonlSetWriter):
    def __init__(self, wb: rocksdict.WriteBatch, cf_handle: rocksdict.ColumnFamily, prefix: str):
        self._wb = wb
        self._cf_handle = cf_handle
        self._prefix = f'{prefix}:set'

    def add(self, k: str):
        key = self.build_key_name(k)

        self._wb.put(key, "1", self._cf_handle)

    def remove(self, k: str):
        key = self.build_key_name(k)

        self._wb.delete(key, self._cf_handle)

    def update(self, s: typing.Set[str]):
        for k in s:
            self.add(k)
