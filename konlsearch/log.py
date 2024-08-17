import datetime
import itertools
import typing

import rocksdict


class SearchLogDto(typing.TypedDict):
    size: int
    token: str


class KonlSearchLog:
    def __init__(self, cf: rocksdict.Rdict):
        self._cf = cf
        self._prefix = "access"
        self._last_second = int(datetime.datetime.now().timestamp())
        self._seq_count_generator = itertools.count(1)

    def generate_seq_id(self) -> int:
        ts = int(datetime.datetime.now().timestamp())

        if self._last_second != ts:
            self._seq_count_generator = itertools.count(1)
            self._last_second = ts

        return f'{ts}:{str(next(self._seq_count_generator)).rjust(4, '0')}'

    def append(self, token: str, size: int) -> None:
        seq_id = self.generate_seq_id()
        key = self.__build_key_name(seq_id, token)
        self._cf[key] = size

    def append_multi(self, requests: typing.List[SearchLogDto]):
        for request in requests:
            self.append(request["token"], request["size"])

    def get_range(self, start_timestamp, end_timestamp):
        if end_timestamp <= start_timestamp:
            return []

        it = self._cf.iter()

        start_key = self.__build_key_id(start_timestamp)
        end_key = self.__build_key_id(end_timestamp)

        it.seek(start_key)

        result = []

        while (it.valid() and type(it.key()) == str and
               it.key().startswith(self._prefix) and it.key() < end_key):
            r = SearchLogDto(
                token=self.__get_token_from_key(it.key()), size=it.value()
            )
            result.append(r)
            it.next()

        return result

    def __build_key_id(self, seq_id: int) -> str:
        return f'{self._prefix}:{seq_id}'

    def __build_key_name(self, seq_id: int, token: str) -> str:
        return f'{self.__build_key_id(seq_id)}:{token}'

    def __get_timestamp_from_key(self, key_with_prefix: str) -> str:
        return key_with_prefix.split(":")[1]

    def __get_token_from_key(self, key_with_prefix: str) -> str:
        return key_with_prefix.split(":")[3]
