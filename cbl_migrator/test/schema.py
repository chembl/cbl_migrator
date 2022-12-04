from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, CheckConstraint
from sqlalchemy.types import String, Integer, Float, Text

Base = declarative_base()


class Compound(Base):
    """
    Non redundant list of compounds/biotherapeutics with associated identifiers
    """

    __tablename__ = "compound"

    cid = Column(
        Integer, primary_key=True, comment="Internal Primary Key for the molecule"
    )
    structure_type = Column(
        Integer, CheckConstraint("structure_type in ('NONE','MOL','SEQ','BOTH')")
    )
    compound_name = Column(String(255), index=True)


class CompoundStructure(Base):

    __tablename__ = "compound_structure"

    sid = Column(Integer, primary_key=True)
    cid = Column(Integer, ForeignKey("compound.cid"))
    smiles = Column(String(4000))
    molblock = Column(Text)
    inchi_key = Column(String(27), unique=True)


class CompoundProperties(Base):

    __tablename__ = "compound_properties"

    pid = Column(Integer, primary_key=True)
    cid = Column(Integer, ForeignKey("compound.cid"))
    mw = Column(Float)
    logp = Column(Float)
