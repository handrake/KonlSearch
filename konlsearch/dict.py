import typing

import rocksdict


class KonlDict:
    def __init__(self, cf: rocksdict.Rdict, prefix: str):
        self._cf = cf
        self._prefix = f'{prefix}:dict'

    def __getitem__(self, k: str) -> str:
        key = self.__build_key_name(k)

        return self._cf[key]

    def __setitem__(self, k: str, v: str) -> None:
        key = self.__build_key_name(k)

        if key not in self._cf:
            self._cf[key] = v

    def __delitem__(self, k: str):
        key = self.__build_key_name(k)

        if key in self._cf:
            self._cf.delete(key)

    def __contains__(self, k: str) -> bool:
        key = self.__build_key_name(k)

        return key in self._cf

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def items(self) -> typing.Generator[tuple[str, str], None, None]:
        it = self._cf.iter()
        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            yield self.__remove_prefix(it.key()), it.value()
            it.next()

    def update(self, d: typing.Dict):
        for k, v in d.items():
            self.__setitem__(k, v)

    def __build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def __remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")


class KonlDictIter:
    def __init__(self, iter: rocksdict.RdictIter, prefix: str):
        self._iter = iter
        self._prefix = f'{prefix}:dict'

    def __getitem__(self, k: str) -> str:
        key = self.__build_key_name(k)

        self._iter.seek(key)

        if self._iter.valid() and self._iter.key() == key:
            return self._iter.value()

        raise KeyError

    def __contains__(self, k: str) -> bool:
        key = self.__build_key_name(k)

        self._iter.seek(key)

        return self._iter.valid() and self._iter.key() == key

    def __len__(self) -> int:
        len = 0

        for _ in self.items():
            len += 1

        return len

    def items(self) -> typing.Generator[tuple[str, str], None, None]:
        it = self._iter()
        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            yield self.__remove_prefix(it.key()), it.value()
            it.next()

    def __build_key_name(self, k: str) -> str:
        return f'{self._prefix}:{k}'

    def __remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._prefix + ":", "")
