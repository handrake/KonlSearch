import rocksdict
import typing
import enum

from . import utility


class TokenSearchMode(enum.StrEnum):
    AND = enum.auto()
    OR = enum.auto()


class KonlInvertedIndex:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = self.__build_inverted_index_name(name)
        self._cf = utility.create_or_get_cf(db, self._name)

    def __getitem__(self, token) -> typing.Set[str]:
        return self._cf[token]

    def close(self):
        self._cf.close()

    def index(self, document_id: int, tokens: typing.Set[str]):
        for token in tokens:
            if token in self._cf:
                self._cf[token] |= {document_id}
            else:
                self._cf[token] = {document_id}

    def delete(self, document_id: int, tokens: typing.Set[str]) -> None:
        for token in tokens:
            if token in self._cf:
                self._cf[token] -= {document_id}

    # noinspection PyBroadException
    def search(self, tokens: typing.List[str], mode: TokenSearchMode) -> typing.List[int]:
        snapshot = self._cf.snapshot()

        result_set = set()

        for i, token in enumerate(tokens):
            try:
                document_ids = snapshot[token]
            except Exception:
                document_ids = set()

            if mode == TokenSearchMode.OR:
                result_set.update(document_ids)
            elif mode == TokenSearchMode.AND:
                if i == 0:
                    result_set.update(document_ids)
                else:
                    result_set.intersection_update(document_ids)

        del snapshot

        return sorted(list(result_set))

    @staticmethod
    def __build_inverted_index_name(name: str) -> str:
        return f'{name}_inverted_index'
