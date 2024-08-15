import re
import typing
import threading
import itertools

import mecab
import rocksdict

from . import utility
from .inverted_index import KonlInvertedIndex, TokenSearchMode
from .lock import StripedLock

mecab = mecab.MeCab()


_SPECIAL_CHARACTERS = '@_!#$%^&*()<>?/\\|}{~:]",'
_LAST_DOCUMENT_ID = "last_document_id"


class IndexGetResponseType(typing.TypedDict):
    id: int
    document: str


class KonlIndex:
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = name
        self._cf = utility.create_or_get_cf(db, name)
        self._inverted_index = KonlInvertedIndex(db, name)
        self._locks = StripedLock(threading.Lock, 10)
        self._prefix = f'{name}:document'
        self._len_prefix = f'{name}:__len__:document'

    def index(self, document) -> int:
        with self._locks.get(self._name):
            it = self._cf.iter()

            sanitized_document = self.sanitize(document)
            tokens = {token for token in set(mecab.morphs(sanitized_document)).union(set(sanitized_document.split()))
                      if self.is_indexable(token)}

            last_document_id = 1

            it.seek(_LAST_DOCUMENT_ID)

            if it.valid() and it.key() == _LAST_DOCUMENT_ID:
                last_document_id = it.value() + 1

            wb = rocksdict.WriteBatch()

            cf_handle = self._db.get_column_family_handle(self._name)
            wb.set_default_column_family(cf_handle)

            wb[_LAST_DOCUMENT_ID] = last_document_id

            key = self.__build_key_name(last_document_id)

            wb[key] = document
            wb[self.__build_token_name(last_document_id)] = tokens

            it.seek(self._len_prefix)

            if it.valid() and it.key() == self._len_prefix:
                size = it.value()
                wb[self._len_prefix]= size + 1
            else:
                wb[self._len_prefix] = last_document_id

            self._inverted_index.toWriteBatch(wb).index(last_document_id, tokens)

            self._cf.write(wb)

        return last_document_id

    def delete(self, document_id) -> None:
        with self._locks.get(self._name):
            document_id_key = self.__build_key_name(document_id)

            if document_id_key not in self._cf:
                raise KeyError

            token_name = self.__build_token_name(document_id)
            tokens = self._cf[token_name]

            self._inverted_index.delete(document_id, tokens)

            self._cf.delete(token_name)

            document_id_key = self.__build_key_name(document_id)
            self._cf.delete(document_id_key)

            size = self.__len__()
            if size > 0:
                self.__set_len(size-1)

    def get(self, document_id) -> IndexGetResponseType:
        document_id_key = self.__build_key_name(document_id)
        return IndexGetResponseType(id=document_id, document=self._cf[document_id_key])

    def get_all(self) -> typing.List[IndexGetResponseType]:
        it = self._cf.iter()
        it.seek(self._prefix)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix):
            r = IndexGetResponseType(id=int(self.__remove_prefix(it.key())), document=it.value())
            result.append(r)
            it.next()

        return result

    def get_range(self, start_id: int, end_id: int) -> typing.List[IndexGetResponseType]:
        if end_id <= start_id:
            return []

        it = self._cf.iter()

        start_key = self.__build_key_name(start_id)
        end_key = self.__build_key_name(end_id)

        it.seek(start_key)

        result = []

        while it.valid() and type(it.key()) == str and it.key().startswith(self._prefix) and self.__remove_prefix(it.key()) < end_id:
            r = IndexGetResponseType(id=int(self.__remove_prefix(it.key())), document=it.value())
            result.append(r)
            it.next()

        return result

    def get_multi(self, document_ids: typing.List[int]) -> typing.List[IndexGetResponseType]:
        keys = [self.__build_key_name(document_id) for document_id in document_ids]

        return [IndexGetResponseType(id=document_ids[i], document=document) for i, document in enumerate(self._cf[keys]) if document]

    def get_tokens(self, document_id) -> typing.Set[str]:
        return self._cf[self.__build_token_name(document_id)]

    # noinspection PyBroadException
    def search(self, tokens: typing.List[str], mode: TokenSearchMode) -> typing.List[int]:
        if mode != TokenSearchMode.PHRASE:
            return self._inverted_index.search(tokens, mode)

        result = self._inverted_index.search(tokens, TokenSearchMode.AND)

        sanitized_tokens = self.__tokenize_with_order(" ".join(tokens))

        tokens_with_ids = [(response["id"], self.__tokenize_with_order(response["document"])) for response in self.get_multi(result)]

        return [tokens_with_id[0] for tokens_with_id in tokens_with_ids if all(x <= y for x, y in itertools.pairwise([tokens_with_id[1].index(token) for token in sanitized_tokens]))]

    def __tokenize_with_order(self, document) -> typing.List[str]:
        sanitized_document = self.sanitize(document)
        return [token for token in list(mecab.morphs(sanitized_document)) if self.is_indexable(token)]

    def search_suggestions(self, prefix: str) -> typing.List[str]:
        return self._inverted_index.search_suggestions(prefix)

    def close(self):
        self._cf.close()
        self._inverted_index.close()

    def __len__(self):
        key = self._len_prefix

        if key in self._cf:
            return self._cf[key]
        else:
            return 0

    def __set_len(self, size: int):
        self._cf[self._len_prefix] = size

    def __remove_prefix(self, key_with_prefix: str) -> int:
        document_id_x = key_with_prefix.replace(self._prefix + ":", "")
        return int(document_id_x, 16)

    def __build_key_name(self, document_id) -> str:
        document_id_s = f'{document_id:x}'.rjust(10, '0')
        return f'{self._prefix}:{document_id_s}'

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
        return ''.join(ch for ch in document if ch not in _SPECIAL_CHARACTERS)
