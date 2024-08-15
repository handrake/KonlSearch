import rocksdict
import typing

from . import utility
from .dict import KonlDict, KonlDictView, KonlDictWriteBatch
from .set import KonlSet, KonlSetView, KonlSetWriteBatch

import hgtk

_TOKEN_DICT = "token_dict"
_TOKEN_REVERSE_DICT = "token_reverse_dict"


def build_trie_name(index_name) -> str:
    return f'{index_name}_trie'


def decompose_word(word: str) -> str:
    return hgtk.text.decompose(text=word, compose_code="")


class KonlTrieView:
    def __init__(self, iter: rocksdict.RdictIter):
        self._iter = iter
        self._token_reverse_dict = KonlDictView(self._iter, _TOKEN_REVERSE_DICT)

    def search(self, prefix: str) -> typing.List[str]:
        decomposed_prefix = decompose_word(prefix)

        return sorted(self.__search(decomposed_prefix))

    def __search(self, decomposed_prefix: str) -> typing.Set[str]:
        s = KonlSetView(self._iter, decomposed_prefix)

        if len(s) == 0:
            return set()

        result_set = set()

        if decomposed_prefix in self._token_reverse_dict:
            result_set.add(self._token_reverse_dict[decomposed_prefix])

        candidate_set = set()

        decomposed_prefix_set = KonlSetView(self._iter, decomposed_prefix)

        for s in decomposed_prefix_set.items():
            candidate_set.add(s)

        for candidate_prefix in candidate_set:
            if candidate_prefix in self._token_reverse_dict:
                result_set.add(self._token_reverse_dict[candidate_prefix])

            result_set.update(self.__search(candidate_prefix))

        return result_set


class KonlTrieWriteBatch:
    def __init__(self, cf: rocksdict.Rdict, wb: rocksdict.WriteBatch):
        self._cf = cf
        self._wb = wb

        iter = self._cf.iter()

        self._token_dict_view = KonlDictView(iter, _TOKEN_DICT)
        self._token_reverse_dict_view = KonlDictView(iter, _TOKEN_REVERSE_DICT)
        self._token_dict_wb = KonlDictWriteBatch(wb, _TOKEN_DICT)
        self._token_reverse_dict_wb = KonlDictWriteBatch(wb, _TOKEN_REVERSE_DICT)

    def insert(self, token) -> None:
        decomposed_token = decompose_word(token)

        for i in range(len(decomposed_token)):
            s = decomposed_token[:i+1]

            if len(s) >= 2:
                s1 = s[:-1]
                set_s1_wb = KonlSetWriteBatch(self._wb, s1)
                set_s1_wb.add(s)

        self._token_dict_wb[token] = decomposed_token
        self._token_reverse_dict_wb[decomposed_token] = token

    def index(self, tokens: typing.Set[str]):
        for token in tokens:
            self.insert(token)

    def delete(self, token) -> None:
        if token not in self._token_dict_view:
            return

        decomposed_token = decompose_word(token)

        if decomposed_token not in self._token_reverse_dict_view:
            return

        for i in range(len(decomposed_token)-1, 1, -1):
            s = decomposed_token[:i+1]

            if len(s) >= 2:
                s1 = s[:-1]
                set_s1_wb = KonlSetWriteBatch(self._wb, s1)
                set_s1_wb.remove(s)

        del self._token_dict_wb[token]
        del self._token_reverse_dict_wb[decomposed_token]


class KonlTrie:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._name = build_trie_name(name)
        self._cf = utility.create_or_get_cf(db, self._name)
        self._token_dict = KonlDict(self._cf, _TOKEN_DICT)
        self._token_reverse_dict = KonlDict(self._cf, _TOKEN_REVERSE_DICT)

    def close(self):
        self._cf.close()

    def to_view(self) -> KonlTrieView:
        return KonlTrieView(self._cf.iter())

    def to_write_batch(self, wb: rocksdict.WriteBatch) -> KonlTrieWriteBatch:
        cf_handle = self._cf.get_column_family_handle(self._name)
        wb.set_default_column_family(cf_handle)
        return KonlTrieWriteBatch(self._cf, wb)

    def insert(self, token) -> None:
        if token in self._token_dict:
            return

        decomposed_token = decompose_word(token)

        for i in range(len(decomposed_token)):
            s = decomposed_token[:i+1]

            if len(s) >= 2:
                s1 = s[:-1]
                set_s1 = KonlSet(self._cf, s1)
                set_s1.add(s)

        self._token_dict[token] = decomposed_token
        self._token_reverse_dict[decomposed_token] = token

    def index(self, tokens: typing.Set[str]):
        for token in tokens:
            self.insert(token)

    def delete(self, token) -> None:
        if token not in self._token_dict:
            return

        decomposed_token = decompose_word(token)

        if decomposed_token not in self._token_reverse_dict:
            return

        for i in range(len(decomposed_token)-1, 1, -1):
            s = decomposed_token[:i+1]

            if len(s) >= 2:
                s1 = s[:-1]
                set_s1 = KonlSet(self._cf, s1)
                set_s1.remove(s)

        del self._token_dict[token]
        del self._token_reverse_dict[decomposed_token]

    def search(self, prefix: str) -> typing.List[str]:
        return self.to_view().search(prefix)
