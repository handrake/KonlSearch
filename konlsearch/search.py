import typing
import enum
import rocksdict
import mecab
from .lock import NamedLock


class KonlSearch:
    def __init__(self, path):
        self.path = path
        self.options = rocksdict.Options()
        options = rocksdict.Options()
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        self.db = rocksdict.Rdict(path=self.path, options=options)
        self.locks = NamedLock()
        self.mecab = mecab.MeCab()

    def create_index(self, name) -> None:
        # noinspection PyBroadException
        try:
            self.db.create_column_family(name)
            self.db.create_column_family(self.__build_inverted_index_name(name))
        except Exception:
            pass

    def index(self, index_name, document) -> int:
        self.locks.acquire(index_name)

        cf = self.db.get_column_family(index_name)
        cf_inverted = self.db.get_column_family(self.__build_inverted_index_name(index_name))

        tokens = set(self.mecab.morphs(document))

        last_document_id = 1

        if "last_document_id" in cf:
            last_document_id = cf["last_document_id"] + 1

        cf["last_document_id"] = last_document_id
        cf[last_document_id] = document
        cf[self.__build_token_name(last_document_id)] = tokens

        for token in tokens:
            if token in cf_inverted:
                cf_inverted[token] = cf_inverted[token] | set([last_document_id])
            else:
                cf_inverted[token] = set([last_document_id])

        self.locks.release(index_name)

        return last_document_id

    def delete(self, index_name, document_id) -> None:
        self.locks.acquire(index_name)

        cf = self.db.get_column_family(index_name)
        cf_inverted = self.db.get_column_family(self.__build_inverted_index_name(index_name))

        if document_id not in cf:
            self.locks.release(index_name)
            raise KeyError

        token_name = self.__build_token_name(document_id)
        tokens = cf[token_name]

        for token in tokens:
            cf_inverted[token].remove(document_id)

            if not cf_inverted[token]:
                cf_inverted.delete(token)

        cf.delete(token_name)
        cf.delete(document_id)

        self.locks.release(index_name)

    def get(self, index_name, document_id) -> str:
        cf = self.db.get_column_family(index_name)

        return cf[document_id]

    def get_tokens(self, index_name, document_id) -> typing.Set[str]:
        cf = self.db.get_column_family(index_name)

        return cf[self.__build_token_name(document_id)]

    # noinspection PyBroadException
    def search(self, index_name, tokens) -> typing.List[int]:
        cf_inverted_snapshot = self.db.get_column_family(self.__build_inverted_index_name(index_name)).snapshot()

        result = []
        result_set = set()

        for token in tokens:
            try:
                document_ids = cf_inverted_snapshot[token]
            except Exception:
                document_ids = set()

            diff = document_ids.difference(result_set)

            if diff:
                result_set.update(document_ids)
                result.extend(list(diff))

        del cf_inverted_snapshot

        return result

    @staticmethod
    def __build_inverted_index_name(index_name) -> str:
        return f'{index_name}_inverted_index'

    @staticmethod
    def __build_token_name(document_id) -> str:
        return f'{document_id}:tokens'

# class SearchOption(enum.Enum):
