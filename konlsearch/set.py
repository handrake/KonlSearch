import typing

import rocksdict


class KonlSet:
    def __init__(self, cf: rocksdict.Rdict, prefix: str):
        self._cf = cf
        self._prefix = f'{prefix}:set'
        self._len_prefix = f'{prefix}:__len__:dict'

    def __contains__(self, k: str) -> bool:
        key = self.__build_key_name(k)

        return key in self._cf

    def __len__(self) -> int:
        key = self._len_prefix

        if key in self._cf:
            return self._cf[key]
        else:
            return 0

    def __set_len(self, size: int):
        self._cf[self._len_prefix] = size

    def add(self, k: str):
        key = self.__build_key_name(k)

        if key not in self._cf:
            self._cf[key] = "1"

            size = self.__len__()
            self.__set_len(size+1)

    def remove(self, k: str):
        key = self.__build_key_name(k)

        if key in self._cf:
            self._cf.delete(key)

            size = self.__len__()
            self.__set_len(size-1)

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
