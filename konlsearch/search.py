import rocksdict

from .index import KonlIndex, KonlIndexFactory


class KonlSearch:
    def __init__(self, path):
        self.path = path
        self.options = rocksdict.Options()
        self.options.create_if_missing(True)
        self.options.create_missing_column_families(True)
        self.db = rocksdict.Rdict(path=self.path, options=self.options)

    def index(self, name) -> KonlIndex:
        return KonlIndexFactory.create_or_get(self.db, name)

    def close(self):
        self.db.close()

    def destroy(self):
        rocksdict.Rdict.destroy(self.path)
