import re
import typing
import threading

import mecab
import rocksdict

from . import utility
from .inverted_index import KonlInvertedIndex, TokenSearchMode
from .lock import StripedLock

mecab = mecab.MeCab()


SPECIAL_CHARACTERS = '@_!#$%^&*()<>?/\\|}{~:]",'


class KonlIndex:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = name
        self._cf = utility.create_or_get_cf(db, name)
        self._inverted_index = KonlInvertedIndex(db, name)
        self._locks = StripedLock(threading.Lock, 10)

    def index(self, document) -> int:
        with self._locks.get(self._name):
            sanitized_document = self.sanitize(document)
            tokens = {token for token in set(mecab.morphs(sanitized_document)).union(set(sanitized_document.split()))
                      if self.is_indexable(token)}

            last_document_id = 1

            if "last_document_id" in self._cf:
                last_document_id = self._cf["last_document_id"] + 1

            self._cf["last_document_id"] = last_document_id
            self._cf[last_document_id] = document
            self._cf[self.__build_token_name(last_document_id)] = list(tokens)

            self._inverted_index.index(last_document_id, tokens)

        return last_document_id

    def delete(self, document_id) -> None:
        with self._locks.get(self._name):
            if document_id not in self._cf:
                raise KeyError

            token_name = self.__build_token_name(document_id)
            tokens = self._cf[token_name]

            self._inverted_index.delete(document_id, tokens)

            self._cf.delete(token_name)
            self._cf.delete(document_id)

    def get(self, document_id) -> str:
        return self._cf[document_id]

    def get_tokens(self, document_id) -> typing.Set[str]:
        return self._cf[self.__build_token_name(document_id)]

    # noinspection PyBroadException
    def search(self, tokens: typing.List[str], mode: TokenSearchMode) -> typing.List[int]:
        return self._inverted_index.search(tokens, mode)

    def close(self):
        self._cf.close()
        self._inverted_index.close()

    @staticmethod
    def __build_token_name(document_id) -> str:
        return f'{document_id}:tokens'

    @staticmethod
    def is_hangul(s) -> bool:
        p = re.compile('[가-힣]+')
        return p.fullmatch(s) is not None

    @staticmethod
    def is_alpha(s) -> bool:
        p = re.compile('[a-zA-Z]+')
        return p.fullmatch(s) is not None

    @staticmethod
    def is_indexable(token):
        return KonlIndex.is_alpha(token) or KonlIndex.is_hangul(token)

    @staticmethod
    def sanitize(document):
        return ''.join(ch for ch in document if ch not in SPECIAL_CHARACTERS)
