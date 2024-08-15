import rocksdict
import typing
import enum

from strenum import StrEnum

from . import utility

from .trie import KonlTrie
from .set import KonlSet, KonlSetView, KonlSetWriteBatch


class TokenSearchMode(StrEnum):
    AND = enum.auto()
    OR = enum.auto()


class KonlInvertedIndex:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = self.__build_inverted_index_name(name)
        self._cf = utility.create_or_get_cf(db, self._name)
        self._trie = KonlTrie(db, name)

    def __getitem__(self, token: str) -> typing.Set[int]:
        s = KonlSetView(self._cf.iter(), token)

        return {int(e) for e in s.items()}

    def __contains__(self, token: str) -> bool:
        s = KonlSetView(self._cf.iter(), token)

        return len(s) > 0

    def close(self):
        self._cf.close()
        self._trie.close()

    def index(self, document_id: int, tokens: typing.Set[str]):
        wb = rocksdict.WriteBatch()
        cf_handle = self._db.get_column_family_handle(self._name)
        wb.set_default_column_family(cf_handle)

        for token in tokens:
            s_wb = KonlSetWriteBatch(wb, token)
            s_wb.add(str(document_id))

            self._trie.insert(token)

        self._cf.write(wb)

    def delete(self, document_id: int, tokens: typing.Set[str]) -> None:
        for token in tokens:
            s = KonlSet(self._cf, token)
            s.remove(str(document_id))

            if len(s) == 0:
                self._trie.delete(token)

    # noinspection PyBroadException
    def search(self, tokens: typing.List[str], mode: TokenSearchMode) -> typing.List[int]:
        result_set = set()

        iter = self._cf.iter()

        for i, token in enumerate(tokens):
            s = KonlSetView(iter, token)

            document_ids = {int(e) for e in s.items()}

            if mode == TokenSearchMode.OR:
                result_set.update(document_ids)
            elif mode == TokenSearchMode.AND:
                if i == 0:
                    result_set.update(document_ids)
                else:
                    result_set.intersection_update(document_ids)

        return sorted(list(result_set))

    def search_suggestions(self, prefix: str) -> typing.List[str]:
        return self._trie.toView().search(prefix)

    @staticmethod
    def __build_inverted_index_name(name: str) -> str:
        return f'{name}_inverted_index'
