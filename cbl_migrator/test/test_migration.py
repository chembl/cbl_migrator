from sqlalchemy import MetaData, create_engine, inspect, insert
from .schema import Base, Compound, CompoundStructure, CompoundProperties
from .. import DbMigrator
import unittest
import random
import os

molblock = """
  SciTegic01111613442D

 13 13  0  0  0  0            999 V2000
    1.2990   -0.7500    0.0000 C   0  0
    1.2990    0.7500    0.0000 C   0  0
    0.0000    1.5000    0.0000 C   0  0
   -1.2990    0.7500    0.0000 C   0  0
   -1.2990   -0.7500    0.0000 C   0  0
    0.0000   -1.5000    0.0000 C   0  0
    0.0031   -3.0008    0.0000 C   0  0
   -1.0351   -3.6026    0.0000 O   0  0
    1.0432   -3.5993    0.0000 O   0  0
   -2.6003   -1.4978    0.0000 O   0  0
   -3.8990   -0.7455    0.0000 C   0  0
   -3.8969    0.4545    0.0000 C   0  0
   -4.9395   -1.3434    0.0000 O   0  0
  1  2  2  0
  2  3  1  0
  3  4  2  0
  4  5  1  0
  5  6  2  0
  6  1  1  0
  6  7  1  0
  7  8  1  0
  7  9  2  0
  5 10  1  0
 10 11  1  0
 11 12  1  0
 11 13  2  0
M  END
"""

inchis = [
    "HCHKCACWOHOZIP-UHFFFAOYSA-N",
    "VNWKTOKETHGBQD-UHFFFAOYSA-N",
    "QAOWNCQODCNURD-UHFFFAOYSA-N",
    "FHLGUOHLUFIAAA-UHFFFAOYSA-N",
    "LVHBHZANLOWSRM-UHFFFAOYSA-N",
    "XEEYBQQBJWHFJM-UHFFFAOYSA-N",
    "XLYOFNOQVPJJNP-UHFFFAOYSA-N",
    "ISWSIDIOOBJBQZ-UHFFFAOYSA-N",
    "XWJBRBSPAODJER-UHFFFAOYSA-N",
    "NTTOTNSKUYCDAV-UHFFFAOYSA-N",
    "IKFRSFGCRAORBO-UHFFFAOYSA-N",
    "NBIIXXVUZAFLBC-UHFFFAOYSA-N",
    "QSHDDOUJBYECFT-UHFFFAOYSA-N",
    "WGLPBDUCMAPZCE-UHFFFAOYSA-N",
    "KKLWSPPIRBIEOV-UHFFFAOYSA-N",
    "YKSNLCVSTHTHJA-UHFFFAOYSA-L",
    "VYPSYNLAJGMNEJ-UHFFFAOYSA-N",
    "XDSSGQHOYWGIKC-UHFFFAOYSA-N",
    "WAQIIHCCEMGYKP-UHFFFAOYSA-N",
    "UORVGPXVDQYIDP-UHFFFAOYSA-N",
    "VGGSQFUCUMXWEO-UHFFFAOYSA-N",
    "IMEVSAIFJKKDAP-UHFFFAOYSA-N",
    "QWTDNUCVQCZILF-UHFFFAOYSA-N",
    "VYZAMTAEIAYCRO-UHFFFAOYSA-N",
    "RRHGJUQNOFWUDK-UHFFFAOYSA-N",
    "NNPPMTNAJDCUHE-UHFFFAOYSA-N",
    "HZZVJAQRINQKSD-PBFISZAISA-N",
    "KWYUFKZDYYNOTN-UHFFFAOYSA-M",
    "AZDRQVAHHNSJOQ-UHFFFAOYSA-N",
    "IAZDPXIOMUYVGZ-UHFFFAOYSA-N",
    "XSQUKJJJFZCRTK-UHFFFAOYSA-N",
    "KFDVPJUYSDEJTH-UHFFFAOYSA-N",
    "RAXXELZNTBOGNW-UHFFFAOYSA-N",
    "JYQUHIFYBATCCY-UHFFFAOYSA-N",
    "LYCAIKOWRPUZTN-UHFFFAOYSA-N",
    "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
    "HQVNEWCFYHHQES-UHFFFAOYSA-N",
    "RYFMWSXOAZQYPI-UHFFFAOYSA-K",
    "QTBSBXVTEAMEQO-UHFFFAOYSA-N",
    "OKKJLVBELUTLKV-UHFFFAOYSA-N",
    "NIXOWILDQLNWCW-UHFFFAOYSA-N",
    "OSSNTDFYBPYIEC-UHFFFAOYSA-N",
]


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)

        self.origin = "sqlite:///origin.db"
        self.dest = "sqlite:///dest.db"

    def __gen_test_data(self):

        # create schema
        engine = create_engine(self.origin)
        Base.metadata.create_all(bind=engine)

        com_stmt = insert(Compound)
        com_struct_stmt = insert(CompoundStructure)
        com_props_stmt = insert(CompoundProperties)

        with engine.begin() as conn:
            conn.execute(
                com_stmt,
                [
                    {"cid": i, "structure_type": "MOL", "compound_name": "chemblone"}
                    for i in range(1, 42)
                ],
            )
            conn.execute(
                com_struct_stmt,
                [
                    {
                        "sid": i,
                        "cid": i,
                        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
                        "molblock": molblock,
                        "inchi_key": inchis[i - 1],
                    }
                    for i in range(1, 42)
                ],
            )
            conn.execute(
                com_props_stmt,
                [
                    {"pid": i, "cid": i, "mw": random.random(), "logp": random.random()}
                    for i in range(1, 42)
                ],
            )

    def __get_tables_insp(self):
        o_eng = create_engine(self.origin)
        d_eng = create_engine(self.dest)

        o_metadata = MetaData()
        o_metadata.reflect(o_eng)
        d_metadata = MetaData()
        d_metadata.reflect(d_eng)

        o_tables = filter(lambda x: x[0], o_metadata.tables.items())
        d_tables = filter(lambda x: x[0], d_metadata.tables.items())
        o_tables = {table_name: table for table_name, table in o_tables}
        d_tables = {table_name: table for table_name, table in d_tables}

        o_insp = inspect(o_eng)
        d_insp = inspect(d_eng)

        return o_tables, d_tables, o_insp, d_insp

    def test_a_migration(self):
        self.__gen_test_data()
        migrator = DbMigrator(self.origin, self.dest)
        self.assertTrue(migrator.migrate(chunk_size=10))

    def test_b_uks(self):
        o_tables, _, o_insp, d_insp = self.__get_tables_insp()

        for table_name, _ in o_tables.items():
            o_uks = o_insp.get_unique_constraints(table_name)
            d_uks = d_insp.get_unique_constraints(table_name)
            self.assertEqual(o_uks, d_uks)

    def test_c_idxs(self):
        o_tables, d_tables, _, _ = self.__get_tables_insp()

        for table_name, table in o_tables.items():
            self.assertEqual(
                [index.name for index in table.indexes],
                [index.name for index in d_tables[table_name].indexes],
            )

    def test_d_pks(self):
        o_tables, d_tables, _, _ = self.__get_tables_insp()

        for table_name, table in o_tables.items():
            self.assertEqual(
                [col.name for col in table.primary_key.columns],
                [col.name for col in d_tables[table_name].primary_key.columns],
            )

    def test_e_fks(self):
        o_tables, _, o_insp, d_insp = self.__get_tables_insp()

        for table_name, _ in o_tables.items():
            o_fks = o_insp.get_foreign_keys(table_name)
            d_fks = d_insp.get_foreign_keys(table_name)
            self.assertEqual(o_fks, d_fks)

    def test_f_cks(self):
        o_tables, _, o_insp, d_insp = self.__get_tables_insp()

        for table_name, _ in o_tables.items():
            o_cks = o_insp.get_check_constraints(table_name)
            d_cks = d_insp.get_check_constraints(table_name)
            self.assertEqual(o_cks, d_cks)

    @classmethod
    def tearDownClass(cls):
        os.remove("origin.db")
        os.remove("dest.db")


if __name__ == "__main__":
    unittest.main()
