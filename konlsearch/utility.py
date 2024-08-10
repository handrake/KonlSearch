import rocksdict


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
