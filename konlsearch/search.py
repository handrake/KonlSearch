import enum
import typing

import rocksdict
from strenum import StrEnum

from .index import KonlIndex


class AccessType(StrEnum):
    READ_WRITE = enum.auto()
    READ_ONLY = enum.auto()


class KonlSearch:
    def __init__(self, path: str, access_type: AccessType = AccessType.READ_WRITE):
        self.path = path
        self.options = rocksdict.Options()
        self.options.create_if_missing(True)
        self.options.create_missing_column_families(True)

        access_type = rocksdict.AccessType.read_write() \
            if (access_type == AccessType.READ_WRITE) else rocksdict.AccessType.read_only()

        self.db = rocksdict.Rdict(path=self.path, options=self.options, access_type=access_type)
        self._index_prefix = "index"

    def index(self, name) -> KonlIndex:
        key = self.__build_index_key(name)

        self.db.put(key, "1")

        return KonlIndex(self.db, name)

    def get_all_indexes(self) -> typing.List[str]:
        it = self.db.iter()
        it.seek(self._index_prefix)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._index_prefix):
            result.append(self.__remove_prefix(it.key()))
            it.next()

        return result

    def close(self):
        self.db.close()

    def destroy(self):
        rocksdict.Rdict.destroy(self.path)

    def __build_index_key(self, name: str) -> str:
        return f'{self._index_prefix}:{name}'

    def __remove_prefix(self, key_with_prefix: str) -> str:
        return key_with_prefix.replace(self._index_prefix + ":", "")
