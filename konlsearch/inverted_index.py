from __future__ import annotations

import rocksdict
import typing
import enum

from strenum import StrEnum

from . import utility

from .log import KonlSearchLog
from .set import KonlSet, KonlSetView, KonlSetWriteBatch
from .trie import KonlTrie


class TokenSearchMode(StrEnum):
    AND = enum.auto()
    OR = enum.auto()
    PHRASE = enum.auto()


class KonlInvertedIndexWriteBatch:
    def __init__(self, inverted_index: KonlInvertedIndex, wb: rocksdict.WriteBatch):
        self._iter = inverted_index._cf.iter()
        self._wb = wb
        self._cf_handle = inverted_index._cf.get_column_family_handle(inverted_index._name)
        self._trie_wb = inverted_index._trie.to_write_batch(wb)

    def index(self, document_id: int, tokens: typing.Set[str]):
        for token in tokens:
            s_wb = KonlSetWriteBatch(self._wb, self._cf_handle, token)
            s_wb.add(str(document_id))

        for token in tokens:
            self._trie_wb.insert(token)

    def delete(self, document_id: int, tokens: typing.Set[str]) -> None:
        for token in tokens:
            s = KonlSetView(self._iter, token)
            s_wb = KonlSetWriteBatch(self._wb, self._cf_handle, token)
            s_wb.remove(str(document_id))

            if document_id in s and len(s) == 1:
                self._trie_wb.delete(token)


class KonlInvertedIndex:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = self.__build_inverted_index_name(name)
        self._cf = utility.create_or_get_cf(db, self._name)
        self._trie = KonlTrie(db, name)
        self._log = KonlSearchLog(self._cf)

    def __getitem__(self, token: str) -> typing.Set[int]:
        s = KonlSetView(self._cf.iter(), token)

        return {int(e) for e in s.items()}

    def __contains__(self, token: str) -> bool:
        s = KonlSetView(self._cf.iter(), token)

        return len(s) > 0

    def close(self):
        self._cf.close()
        self._trie.close()

    def to_write_batch(self, wb: rocksdict.WriteBatch):
        return KonlInvertedIndexWriteBatch(self, wb)

    def index(self, document_id: int, tokens: typing.Set[str]):
        wb = rocksdict.WriteBatch()
        cf_handle = self._db.get_column_family_handle(self._name)

        for token in tokens:
            s_wb = KonlSetWriteBatch(wb, cf_handle, token)
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

            if document_ids:
                self._log.append(token, 1)

            if mode == TokenSearchMode.OR or i == 0:
                result_set.update(document_ids)
            elif mode == TokenSearchMode.AND:
                result_set.intersection_update(document_ids)

        return sorted(list(result_set))

    def search_suggestions(self, prefix: str) -> typing.List[str]:
        return self._trie.to_view().search(prefix)

    @staticmethod
    def __build_inverted_index_name(name: str) -> str:
        return f'{name}_inverted_index'
