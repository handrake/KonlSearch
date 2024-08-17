import itertools
import typing

import rocksdict

T = typing.TypeVar("T")


def create_cf(db: rocksdict.Rdict, name: str) -> rocksdict.Rdict:
    return db.create_column_family(name)


def get_cf(db: rocksdict.Rdict, name: str) -> rocksdict.Rdict:
    return db.get_column_family(name)


# noinspection PyBroadException
def create_or_get_cf(db: rocksdict.Rdict, name: str) -> rocksdict.Rdict:
    try:
        return create_cf(db, name)
    except Exception:
        return get_cf(db, name)


def is_sorted(list: typing.List[T]) -> bool:
    return all(x <= y for x, y in itertools.pairwise(list))
