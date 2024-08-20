from dataclasses import dataclass
import datetime
import itertools
import typing

import rocksdict


@dataclass
class SearchLogRequest:
    size: int
    token: str


@dataclass
class SearchLogResponse:
    seq_id: str
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

        seq_count_s = str(next(self._seq_count_generator)).rjust(4, '0')

        return f'{ts}:{seq_count_s}'

    def append(self, token: str, size: int) -> None:
        seq_id = self.generate_seq_id()
        key = self.__build_key_name(seq_id, token)
        self._cf[key] = size

    def append_multi(self, requests: typing.List[SearchLogRequest]):
        for request in requests:
            self.append(request.token, request.size)

    def get_range(self, start_timestamp: int, end_timestamp: int) -> typing.List[SearchLogResponse]:
        if end_timestamp <= start_timestamp:
            return []

        it = self._cf.iter()

        start_key = self.__build_key_id(start_timestamp)
        end_key = self.__build_key_id(end_timestamp)

        it.seek(start_key)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix) and it.key() < end_key:
            r = SearchLogResponse(
                seq_id=self.__get_seq_id_from_key(it.key()),
                token=self.__get_token_from_key(it.key()),
                size=it.value()
            )
            result.append(r)
            it.next()

        return result

    def get_range_seq_id(self, start_seq_id: str, end_seq_id: str) -> typing.List[SearchLogResponse]:
        if end_seq_id <= start_seq_id:
            return []

        it = self._cf.iter()

        start_key = self.__build_key_seq_id(start_seq_id)
        end_key = self.__build_key_seq_id(end_seq_id)

        it.seek(start_key)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix) and it.key() < end_key:
            r = SearchLogResponse(
                seq_id=self.__get_seq_id_from_key(it.key()),
                token=self.__get_token_from_key(it.key()),
                size=it.value()
            )
            result.append(r)
            it.next()

        return result

    def get_first_seq_id(self) -> typing.Optional[str]:
        it = self._cf.iter()

        it.seek(self._prefix)

        if it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            return self.__get_seq_id_from_key(it.key())
        else:
            return None

    def get_last_seq_id(self) -> typing.Optional[str]:
        it = self._cf.iter()

        it.seek(self._prefix)

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            it.next()

        it.prev()

        if it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            return self.__get_seq_id_from_key(it.key())
        else:
            return None

    def __build_key_id(self, timestamp: int) -> str:
        return f'{self._prefix}:{timestamp}'

    def __build_key_seq_id(self, seq_id: str) -> str:
        return f'{self._prefix}:{seq_id}'

    def __build_key_name(self, seq_id: int, token: str) -> str:
        return f'{self.__build_key_id(seq_id)}:{token}'

    def __get_timestamp_from_key(self, key_with_prefix: str) -> str:
        return key_with_prefix.split(":")[1]

    def __get_token_from_key(self, key_with_prefix: str) -> str:
        return key_with_prefix.split(":")[3]

    def __get_seq_id_from_key(self, key_with_prefix: str) -> str:
        parts = key_with_prefix.split(":")
        return f'{parts[1]}:{parts[2]}'
