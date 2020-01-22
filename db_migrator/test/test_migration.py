from sqlalchemy import MetaData, create_engine, ForeignKey
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String
from .. import DbMigrator
import unittest


class TestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)
        self.engine_origin = create_engine('sqlite:///origin.db')

        metadata = MetaData()

        table = Table('t1', metadata,
            Column('aa', Integer, primary_key=True),
            Column('bb', String(60), nullable=False),
            Column('cc', Integer))              

        metadata.create_all(self.engine_origin)

        stmt = table.insert([{"aa": x, "bb": "asdasda", "cc": 1} for x in range(1, 4353)])
        self.engine_origin.execute(stmt)


    def test_migration(self):

        origin = 'sqlite:///origin.db'
        dest = 'sqlite:///dest.db'

        migrator = DbMigrator(origin, dest)

        self.assertTrue(migrator.migrate())
