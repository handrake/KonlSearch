import abc
import typing

import rocksdict


class AbstractKonlDict(abc.ABC):
    def build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")


class KonlDictReader(AbstractKonlDict):
    @abc.abstractmethod
    def __contains__(self, k: str) -> bool:
        pass

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def items(self) -> typing.Generator[tuple[str, str], None, None]:
        if hasattr(self, "_iter"):
            it = self._iter()
        else:
            it = self._cf.iter()
        it.seek(self._prefix)

        while (it.valid() and type(it.key()) == str and
               it.key().startswith(self._prefix)):
            yield self.remove_prefix(it.key()), it.value()
            it.next()


class KonlDictWriter(AbstractKonlDict):
    @abc.abstractmethod
    def __setitem__(self, k: str, v: str) -> None:
        pass

    @abc.abstractmethod
    def __delitem__(self, k: str):
        pass

    @abc.abstractmethod
    def update(self, d: typing.Dict):
        pass


class KonlDictView(KonlDictReader):
    def __init__(self, iter: rocksdict.RdictIter, prefix: str):
        self._iter = iter
        self._prefix = f'{prefix}:dict'

    def __getitem__(self, k: str) -> str:
        key = self.build_key_name(k)

        self._iter.seek(key)

        if self._iter.valid() and self._iter.key() == key:
            return self._iter.value()

        raise KeyError

    def __contains__(self, k: str) -> bool:
        key = self.build_key_name(k)

        self._iter.seek(key)

        return self._iter.valid() and self._iter.key() == key


class KonlDict(KonlDictReader, KonlDictWriter):
    def __init__(self, cf: rocksdict.Rdict, prefix: str):
        self._cf = cf
        self._prefix = f'{prefix}:dict'

    def __getitem__(self, k: str) -> str:
        key = self.build_key_name(k)

        return self._cf[key]

    def __setitem__(self, k: str, v: str) -> None:
        key = self.build_key_name(k)

        self._cf[key] = v

    def __delitem__(self, k: str):
        key = self.build_key_name(k)

        if key in self._cf:
            self._cf.delete(key)

    def __contains__(self, k: str) -> bool:
        key = self.build_key_name(k)

        return key in self._cf

    def update(self, d: typing.Dict):
        for k, v in d.items():
            self.__setitem__(k, v)

    def to_view(self):
        iter = self._cf.iter()

        return KonlDictView(iter, self._prefix)


class KonlDictWriteBatch(KonlDictWriter):
    def __init__(self, wb: rocksdict.WriteBatch, cf_handle: rocksdict.ColumnFamily, prefix: str):
        self._wb = wb
        self._cf_handle = cf_handle
        self._prefix = f'{prefix}:dict'

    def __setitem__(self, k: str, v: str) -> None:
        key = self.build_key_name(k)

        self._wb.put(key, v, self._cf_handle)

    def __delitem__(self, k: str):
        key = self.build_key_name(k)

        self._wb.delete(key, self._cf_handle)

    def update(self, d: typing.Dict):
        for k, v in d.items():
            self.__setitem__(k, v)
