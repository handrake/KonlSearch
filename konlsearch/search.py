import rocksdict

from .index import KonlIndex, KonlIndexFactory


class KonlSearch:
    def __init__(self, path):
        self.path = path
        self.options = rocksdict.Options()
        options = rocksdict.Options()
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        self.db = rocksdict.Rdict(path=self.path, options=options)

    def get_index(self, name) -> KonlIndex:
        return KonlIndexFactory.get(self.db, name)

    def create_index(self, name) -> KonlIndex:
        return KonlIndexFactory.create(self.db, name)

    def create_or_get_index(self, name) -> KonlIndex:
        return KonlIndexFactory.create_or_get(self.db, name)

# class SearchOption(enum.Enum):
