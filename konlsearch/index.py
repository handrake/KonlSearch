from __future__ import annotations

import abc
from dataclasses import dataclass
import enum
import re
import typing
import typing_extensions
import threading

import mecab
import rocksdict

from strenum import StrEnum

import xxhash

from . import utility
from .inverted_index import KonlInvertedIndex, TokenSearchMode
from .lock import StripedLock
from .dict import KonlDict, KonlDictView, KonlDictWriteBatch


mecab = mecab.MeCab()


_SPECIAL_CHARACTERS = '@_!#$%^&*()<>?/\\|}{~:]",'
_LAST_DOCUMENT_ID = "last_document_id"


class SearchMode(StrEnum):
    AND = enum.auto()
    OR = enum.auto()


class IndexingStatusCode(StrEnum):
    SUCCESS = enum.auto()
    CONFLICT = enum.auto()


@dataclass
class IndexGetResponse:
    id: int
    document: str


@dataclass
class SearchGetRequest:
    tokens: typing.List[str]
    mode: TokenSearchMode


@dataclass
class ComplexSearchGetRequest:
    condition1: typing.Union[SearchGetRequest, typing_extensions.Self]
    condition2: typing.Union[SearchGetRequest, typing_extensions.Self]
    mode: SearchMode


@dataclass
class IndexingResult:
    status_code: IndexingStatusCode
    document_id: typing.Optional[int]

    @staticmethod
    def success(document_id: int) -> typing_extensions.Self:
        return IndexingResult(status_code=IndexingStatusCode.SUCCESS, document_id=document_id)

    @staticmethod
    def conflict(document_id: int) -> typing_extensions.Self:
        return IndexingResult(status_code=IndexingStatusCode.CONFLICT, document_id=document_id)


class KonlIndexWriter(abc.ABC):
    def build_key_name(self, document_id) -> str:
        document_id_s = f'{document_id:x}'.rjust(10, '0')
        return f'{self._prefix}:{document_id_s}'

    def tokenize(self, document) -> typing.Set[str]:
        sanitized_document = self.sanitize(document)

        return {token for token in
                set(mecab.morphs(sanitized_document))
                .union(set(sanitized_document.split()))
                if self.is_indexable(token)}

    def generate_hash(self, document) -> str:
        return xxhash.xxh128(document).hexdigest()

    @staticmethod
    def build_token_name(document_id) -> str:
        return f'{document_id}:tokens'

    @staticmethod
    def sanitize(document):
        return ''.join(ch for ch in document if ch not in _SPECIAL_CHARACTERS)

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


class KonlIndexWriteBatch(KonlIndexWriter):
    def __init__(self, index: KonlIndex, wb: rocksdict.WriteBatch):
        self._cf = index._cf
        self._iter = self._cf.iter()
        self._wb = wb
        self._name = index._name
        self._inverted_index = index._inverted_index
        self._locks = index._locks
        self._prefix = f'{index._name}:document'
        self._len_prefix = f'{index._name}:__len__:document'
        self._hash_prefix = f'{index._name}:hash'

    def get_document_id_from_hash(self, hash: str) -> typing.Optional[int]:
        d = KonlDictView(self._iter, self._hash_prefix)

        try:
            return d[hash]
        except KeyError:
            return None

    def add_document_hash(self, document_id: int, hash: str):
        d = KonlDictWriteBatch(self._wb, self._hash_prefix)

        d[hash] = document_id

    def index(self, document) -> IndexingResult:
        with (self._locks.get(self._name)):
            document_hash = self.generate_hash(document)
            conflicting_document_id = self.get_document_id_from_hash(document_hash)

            if conflicting_document_id:
                return IndexingResult.conflict(conflicting_document_id)

            it = self._iter

            tokens = self.tokenize(document)

            last_document_id = 1

            it.seek(_LAST_DOCUMENT_ID)

            if it.valid() and it.key() == _LAST_DOCUMENT_ID:
                last_document_id = it.value() + 1

            self._wb[_LAST_DOCUMENT_ID] = last_document_id

            key = self.build_key_name(last_document_id)

            self._wb[key] = document
            self._wb[self.build_token_name(last_document_id)] = tokens

            it.seek(self._len_prefix)

            if it.valid() and it.key() == self._len_prefix:
                size = it.value()
                self._wb[self._len_prefix] = size + 1
            else:
                self._wb[self._len_prefix] = last_document_id

            inverted_index_wb = self._inverted_index.to_write_batch(self._wb)
            inverted_index_wb.index(last_document_id, tokens)

            self.add_document_hash(last_document_id, document_hash)

        return IndexingResult.success(last_document_id)

    def commit(self):
        self._cf.write(self._wb)

class KonlIndex(KonlIndexWriter):
    def __init__(self, db: rocksdict.Rdict, name: str):
        self._db = db
        self._name = name
        self._cf = utility.create_or_get_cf(db, name)
        self._inverted_index = KonlInvertedIndex(db, name)
        self._locks = StripedLock(threading.Lock, 10)
        self._prefix = f'{name}:document'
        self._len_prefix = f'{name}:__len__:document'
        self._hash_prefix = f'{name}:hash'

    def get_document_id_from_hash(self, hash: str) -> typing.Optional[int]:
        d = KonlDict(self._cf, self._hash_prefix)

        try:
            return d[hash]
        except KeyError:
            return None

    def add_document_hash(self, document_id: int, hash: str):
        d = KonlDict(self._cf, self._hash_prefix)

        d[hash] = document_id

    def delete_document_hash(self, hash: str):
        d = KonlDict(self._cf, self._hash_prefix)

        del d[hash]

    def index(self, document) -> IndexingResult:
        with self._locks.get(self._name):
            document_hash = self.generate_hash(document)
            conflicting_document_id = self.get_document_id_from_hash(document_hash)

            if conflicting_document_id:
                return IndexingResult.conflict(conflicting_document_id)

            tokens = self.tokenize(document)

            last_document_id = 1

            if _LAST_DOCUMENT_ID in self._cf:
                last_document_id = self._cf[_LAST_DOCUMENT_ID] + 1

            self._cf[_LAST_DOCUMENT_ID] = last_document_id

            key = self.build_key_name(last_document_id)

            self._cf[key] = document
            self._cf[self.build_token_name(last_document_id)] = tokens
            size = self.__len__()
            self.__set_len(size+1)

            self._inverted_index.index(last_document_id, tokens)

            self.add_document_hash(last_document_id, document_hash)

        return IndexingResult.success(last_document_id)

    def to_write_batch(self):
        wb = rocksdict.WriteBatch()
        cf_handle = self._cf.get_column_family_handle(self._name)
        wb.set_default_column_family(cf_handle)
        return KonlIndexWriteBatch(self, wb)

    def delete(self, document_id) -> None:
        with self._locks.get(self._name):
            document_id_key = self.build_key_name(document_id)

            if document_id_key not in self._cf:
                raise KeyError

            get_response = self.get(document_id)

            document_hash = self.generate_hash(get_response.document)
            self.delete_document_hash(document_hash)

            token_name = self.build_token_name(document_id)
            tokens = self._cf[token_name]

            self._inverted_index.delete(document_id, tokens)

            self._cf.delete(token_name)

            document_id_key = self.build_key_name(document_id)
            self._cf.delete(document_id_key)

            size = self.__len__()
            if size > 0:
                self.__set_len(size-1)

    def commit(self, wb: rocksdict.WriteBatch):
        self._cf.write(wb)

    def get(self, document_id) -> IndexGetResponse:
        document_id_key = self.build_key_name(document_id)
        return IndexGetResponse(
            id=document_id, document=self._cf[document_id_key]
        )

    def get_all(self) -> typing.List[IndexGetResponse]:
        it = self._cf.iter()
        it.seek(self._prefix)

        result = []

        while (it.valid() and type(it.key()) == str and it.key().startswith(self._prefix)):
            r = IndexGetResponse(id=int(self.__remove_prefix(it.key())), document=it.value())
            result.append(r)
            it.next()

        return result

    def get_range(self, start_id: int,
                  end_id: int) -> typing.List[IndexGetResponse]:
        if end_id <= start_id:
            return []

        it = self._cf.iter()

        start_key = self.build_key_name(start_id)
        end_key = self.build_key_name(end_id)

        it.seek(start_key)

        result = []

        while (it.valid() and type(it.key()) == str and it.key().startswith(self._prefix) and it.key() < end_key):
            r = IndexGetResponse(id=int(self.__remove_prefix(it.key())), document=it.value())
            result.append(r)
            it.next()

        return result

    def get_multi(
            self,
            document_ids: typing.List[int]
    ) -> typing.List[IndexGetResponse]:
        keys = [self.build_key_name(document_id) for document_id in document_ids]

        return [IndexGetResponse(id=document_ids[i], document=document)
                for i, document in enumerate(self._cf[keys]) if document]

    def get_tokens(self, document_id) -> typing.Set[str]:
        return self._cf[self.build_token_name(document_id)]

    def search_complex(
            self, request: ComplexSearchGetRequest
    ) -> typing.List[int]:
        if isinstance(request.condition1, ComplexSearchGetRequest):
            result1 = self.search_complex(request.condition1)
        else:
            result1 = self.search(request.condition1.tokens, request.condition1.mode)

        if isinstance(request.condition2, ComplexSearchGetRequest):
            result2 = self.search_complex(request.condition2)
        else:
            result2 = self.search(request.condition2.tokens, request.condition2.mode)

        if request.mode == SearchMode.AND:
            return sorted(set(result1).intersection(set(result2)))
        elif request.mode == SearchMode.OR:
            return sorted(set(result1).union(set(result2)))
        else:
            return []

    # noinspection PyBroadException
    def search(self, tokens: typing.List[str], mode: TokenSearchMode) -> typing.List[int]:
        if mode != TokenSearchMode.PHRASE:
            return self._inverted_index.search(tokens, mode)

        result = self._inverted_index.search(tokens, TokenSearchMode.AND)

        sanitized_tokens = self.__tokenize_with_order(" ".join(tokens))

        tokens_with_ids = [(response.id,
                            self.__tokenize_with_order(response.document))
                           for response in self.get_multi(result)]

        return [tokens_with_id[0] for tokens_with_id in tokens_with_ids
                if utility.is_sorted([tokens_with_id[1].index(token) for token in sanitized_tokens])]

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
