import itertools
import typing

import rocksdict


_LAST_LOG_ID = "last_log_id"


class SearchLogDto(typing.TypedDict):
    size: int
    token: str


class KonlSearchLog:
    def __init__(self, cf: rocksdict.Rdict):
        self._cf = cf
        self._prefix = f'access'
        if _LAST_LOG_ID in self._cf:
            self._last_log_id_count = itertools.count(self._cf[_LAST_LOG_ID])
        else:
            self._last_log_id_count = itertools.count(1)
        self._last_log_id = next(self._last_log_id_count)

    def append(self, token: str, size: int) -> None:
        self.__append(token, size)
        self._cf[_LAST_LOG_ID] = self._last_log_id

    def __append(self, token: str, size: int) -> None:
        key = self.__build_key_name(token)
        self._cf[key] = size
        self._last_log_id = next(self._last_log_id_count)

    def append_multi(self, requests: typing.List[SearchLogDto]):
        for request in requests:
            self.__append(request["token"], request["size"])

    def get_range(self, start_id, end_id):
        if end_id <= start_id:
            return []

        it = self._cf.iter()

        start_key = self.__build_key_id(start_id)
        end_key = self.__build_key_id(end_id)

        it.seek(start_key)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix) and self.__remove_prefix(it.key()) < end_id:
            r = SearchLogDto(token=self.__remove_prefix_id(it.key()), size=it.value())
            result.append(r)
            it.next()

        return result

    def __build_key_id(self, log_id: int) -> str:
        log_id_x = f'{log_id:x}'.rjust(10, '0')
        return f'{self._prefix}:{log_id_x}'

    def __build_key_name(self, token: str) -> str:
        log_id = f'{self._last_log_id:x}'.rjust(10, '0')
        return f'{self._prefix}:{log_id}:{token}'

    def __remove_prefix(self, key_with_prefix: str) -> int:
        log_id_x = key_with_prefix.split(":")[1]
        return int(log_id_x, 16)

    def __remove_prefix_id(self, key_with_prefix: str) -> str:
        return key_with_prefix.split(":")[2]