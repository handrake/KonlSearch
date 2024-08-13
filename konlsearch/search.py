import typing

import rocksdict

from .index import KonlIndex


class KonlSearch:
    def __init__(self, path):
        self.path = path
        self.options = rocksdict.Options()
        self.options.create_if_missing(True)
        self.options.create_missing_column_families(True)
        self.db = rocksdict.Rdict(path=self.path, options=self.options)

    def index(self, name) -> KonlIndex:
        return KonlIndex(self.db, name)

    def get_all_indexes(self) -> typing.List[str]:
        return self.db.list_cf(self.path)

    def close(self):
        self.db.close()

    def destroy(self):
        rocksdict.Rdict.destroy(self.path)
