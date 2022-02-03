from sqlalchemy.types import Numeric, Text, BigInteger, SmallInteger, Integer
from sqlalchemy.dialects.mysql import (
    TINYINT as mysql_TINYINT,
    SMALLINT as mysql_SMALLINT,
    MEDIUMINT as mysql_MEDIUMINT,
    INTEGER as mysql_INTEGER,
    BIGINT as mysql_BIGINT,
    LONGTEXT as mysql_LONGTEXT,
)


COLTYPE_CONV = {}


def ora2mysql(coltype):
    """
    Used in ChEMBL dump generation
    """
    if isinstance(coltype, Numeric):
        if coltype.scale == 0:
            if coltype.precision == 1:
                coltype = mysql_TINYINT()
            elif coltype.precision == 2:
                coltype = mysql_SMALLINT()
            elif coltype.precision == 3:
                coltype = mysql_MEDIUMINT()
            elif coltype.precision == 4:
                coltype = mysql_INTEGER()
            else:
                coltype = mysql_BIGINT()
        else:
            if not coltype.precision and not coltype.scale:
                coltype.precision = 64  # max mysql precision
                coltype.scale = 30  # max mysql scale
    elif isinstance(coltype, Text):
        coltype = coltype.adapt(mysql_LONGTEXT)
    return coltype


def ora2pg(coltype):
    """
    Used in ChEMBL dump generation
    """
    if isinstance(coltype, Numeric):
        if not coltype.precision or coltype.precision > 4:
            coltype = coltype.adapt(BigInteger)
        else:
            if coltype.precision <= 2:
                coltype = coltype.adapt(SmallInteger)
            elif 2 < coltype.precision <= 4:
                coltype = coltype.adapt(Integer)
    return coltype


def ora2sqlite(coltype):
    """
    Used in ChEMBL dump generation
    """
    if isinstance(coltype, Numeric):
        if coltype.scale == 0:
            if not coltype.precision or coltype.precision > 4:
                coltype = coltype.adapt(BigInteger)
            else:
                if coltype.precision <= 2:
                    coltype = coltype.adapt(SmallInteger)
                elif 2 < coltype.precision <= 4:
                    coltype = coltype.adapt(Integer)
    return coltype


def sqlite2ora(coltype):
    """
    Not much tested
    """
    return coltype


def sqlite2sqlite(coltype):
    """
    Used to run the GA simple tests
    """
    return coltype


def pg2ora(coltype):
    """
    Not much tested
    """
    return coltype


def mysql2ora(coltype):
    """
    Not much tested
    """
    return coltype


COLTYPE_CONV["oracle"] = {"mysql": ora2mysql, "postgresql": ora2pg, "sqlite": ora2sqlite}
COLTYPE_CONV["sqlite"] = {"oracle": sqlite2ora, "sqlite": sqlite2sqlite}
COLTYPE_CONV["postgresql"] = {"oracle": pg2ora}
COLTYPE_CONV["mysql"] = {"oracle": mysql2ora}
