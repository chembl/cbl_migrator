from sqlalchemy.types import Numeric, Text, BigInteger, SmallInteger, Integer
from sqlalchemy.dialects.mysql import (
    TINYINT as mysql_TINYINT,
    SMALLINT as mysql_SMALLINT,
    MEDIUMINT as mysql_MEDIUMINT,
    INTEGER as mysql_INTEGER,
    BIGINT as mysql_BIGINT,
    LONGTEXT as mysql_LONGTEXT,
)
from sqlalchemy.dialects.mysql import REAL as sqlite_REAL


CONV = {}


def ora2mysql(col):
    if isinstance(col.type, Numeric):
        if col.type.scale == 0:
            if col.type.precision == 1:
                col.type = mysql_TINYINT()
            elif col.type.precision == 2:
                col.type = mysql_SMALLINT()
            elif col.type.precision == 3:
                col.type = mysql_MEDIUMINT()
            elif col.type.precision == 4:
                col.type = mysql_INTEGER()
            else:
                col.type = mysql_BIGINT()
        else:
            if not col.type.precision and not col.type.scale:
                col.type.precision = 64  # max mysql precision
                col.type.scale = 30  # max mysql scale
    elif isinstance(col.type, Text):
        col.type = col.type.adapt(mysql_LONGTEXT)


def ora2pg(col):
    if isinstance(col.type, Numeric):
        if not col.type.precision or col.type.precision > 4:
            col.type = col.type.adapt(BigInteger)
        else:
            if col.type.precision <= 2:
                col.type = col.type.adapt(SmallInteger)
            elif 2 < col.type.precision <= 4:
                col.type = col.type.adapt(Integer)


def ora2sqlite(col):
    if isinstance(col.type, Numeric):
        if col.type.scale == 0:
            if not col.type.precision or col.type.precision > 4:
                col.type = col.type.adapt(BigInteger)
            else:
                if col.type.precision <= 2:
                    col.type = col.type.adapt(SmallInteger)
                elif 2 < col.type.precision <= 4:
                    col.type = col.type.adapt(Integer)
        else:
            col.type = col.type.adapt(sqlite_REAL)
    return col


def sqlite2ora(col):
    return col


CONV["oracle"] = {"mysql": ora2mysql, "postgresql": ora2pg, "sqlite": ora2sqlite}
CONV["sqlite"] = {"oracle": sqlite2ora}
