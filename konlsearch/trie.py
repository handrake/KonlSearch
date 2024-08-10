import rocksdict
import typing

from . import utility

import hgtk

_TOKEN_DICT = "token_dict"
_TOKEN_REVERSE_DICT = "token_reverse_dict"


class KonlTrie:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._name = self.__build_trie_name(name)
        self._cf = utility.create_or_get_cf(db, self._name)

    def close(self):
        self._cf.close()

    def insert(self, token) -> None:
        if _TOKEN_DICT not in self._cf:
            self._cf[_TOKEN_DICT] = {}

        token_dict = self._cf[_TOKEN_DICT]

        if token in token_dict:
            return

        if _TOKEN_REVERSE_DICT not in self._cf:
            self._cf[_TOKEN_REVERSE_DICT] = {}

        token_reverse_dict = self._cf[_TOKEN_REVERSE_DICT]

        decomposed_token = self.__decompose_word(token)

        for i in range(len(decomposed_token)):
            s = decomposed_token[:i+1]

            if s not in self._cf:
                self._cf[s] = set()

            if len(s) >= 2:
                self._cf[s[:-1]] |= {s}

        token_dict[token] = decomposed_token
        token_reverse_dict[decomposed_token] = token

        self._cf[_TOKEN_DICT] = token_dict
        self._cf[_TOKEN_REVERSE_DICT] = token_reverse_dict

    def index(self, tokens: typing.Set[str]):
        for token in tokens:
            self.insert(token)

    def delete(self, token) -> None:
        if _TOKEN_DICT not in self._cf or token not in self._cf[_TOKEN_DICT]:
            return

        decomposed_token = self.__decompose_word(token)

        if decomposed_token not in self._cf[_TOKEN_REVERSE_DICT]:
            return

        for i in range(len(decomposed_token)-1, 1, -1):
            s = decomposed_token[:i+1]

            if not self._cf[s]:
                self._cf.delete(s)

        token_dict = self._cf[_TOKEN_DICT]
        token_reverse_dict = self._cf[_TOKEN_REVERSE_DICT]

        del token_dict[token]
        del token_reverse_dict[decomposed_token]

        self._cf[_TOKEN_DICT] = token_dict
        self._cf[_TOKEN_REVERSE_DICT] = token_reverse_dict

    def search(self, prefix: str) -> typing.List[str]:
        if _TOKEN_DICT not in self._cf or _TOKEN_REVERSE_DICT not in self._cf:
            return []

        decomposed_prefix = self.__decompose_word(prefix)

        return sorted(self._search(decomposed_prefix))

    def _search(self, decomposed_prefix: str) -> typing.Set[str]:
        if decomposed_prefix not in self._cf:
            return set()

        result_set = set()

        if decomposed_prefix in self._cf[_TOKEN_REVERSE_DICT]:
            result_set.add(self._cf[_TOKEN_REVERSE_DICT][decomposed_prefix])

        candidate_set = set()

        for s in self._cf[decomposed_prefix]:
            candidate_set.add(s)

        for candidate_prefix in candidate_set:
            if candidate_prefix in self._cf[_TOKEN_REVERSE_DICT]:
                result_set.add(self._cf[_TOKEN_REVERSE_DICT][candidate_prefix])

            result_set.update(self._search(candidate_prefix))

        return result_set

    def close(self):
        self._cf.close()

    @staticmethod
    def __build_trie_name(index_name):
        return f'{index_name}_trie'

    @staticmethod
    def __decompose_word(word: str) -> str:
        return hgtk.text.decompose(text=word, compose_code="")
