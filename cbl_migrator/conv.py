from sqlalchemy.types import Numeric, Text, String, BigInteger, SmallInteger, Integer
from sqlalchemy.dialects.oracle import CLOB as ora_clob
from sqlalchemy.dialects.mysql import (
    TINYINT as mysql_TINYINT,
    SMALLINT as mysql_SMALLINT,
    MEDIUMINT as mysql_MEDIUMINT,
    INTEGER as mysql_INTEGER,
    BIGINT as mysql_BIGINT,
    LONGTEXT as mysql_LONGTEXT,
)


COLTYPE_CONV = {}


def ora2mysql(col):
    """
    Used in ChEMBL dump generation
    """
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
                col.type.precision = 64
                col.type.scale = 30
    elif isinstance(col.type, Text):
        col.type = col.type.adapt(mysql_LONGTEXT)
    return col


def ora2pg(col):
    """
    Used in ChEMBL dump generation
    """
    if isinstance(col.type, Numeric):
        if col.type.scale == 0:
            if not col.type.precision or col.type.precision > 4:
                col.type = col.type.adapt(BigInteger)
            else:
                if col.type.precision <= 2:
                    col.type = col.type.adapt(SmallInteger)
                elif 2 < col.type.precision <= 4:
                    col.type = col.type.adapt(Integer)
    return col


def ora2sqlite(col):
    """
    Used in ChEMBL dump generation
    """
    if isinstance(col.type, Numeric):
        if col.type.scale == 0:
            if not col.type.precision or col.type.precision > 4:
                col.type = col.type.adapt(BigInteger)
            else:
                if col.type.precision <= 2:
                    col.type = col.type.adapt(SmallInteger)
                elif 2 < col.type.precision <= 4:
                    col.type = col.type.adapt(Integer)
    return col


def sqlite2ora(col):
    """
    Not much tested
    """
    return col


def sqlite2sqlite(col):
    """
    Used to run the GA simple tests
    """
    return col


def pg2ora(col):
    """
    Not much tested
    """
    return col


def mysql2ora(col):
    """
    Not much tested
    """
    if isinstance(col.type, String):
        if not col.type.length:
            col.type = col.type.adapt(ora_clob)
    if isinstance(col.type, Numeric):
        if col.type.precision > 30:
            col.type.precision = 30
    return col


COLTYPE_CONV["oracle"] = {
    "mysql": ora2mysql,
    "postgresql": ora2pg,
    "sqlite": ora2sqlite,
}
COLTYPE_CONV["sqlite"] = {"oracle": sqlite2ora, "sqlite": sqlite2sqlite}
COLTYPE_CONV["postgresql"] = {"oracle": pg2ora}
