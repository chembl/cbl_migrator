from sqlalchemy.orm import Session
from sqlalchemy.sql.base import ColumnCollection
from sqlalchemy.util._collections import immutabledict
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.schema import AddConstraint
from sqlalchemy.sql import select
from sqlalchemy.types import Numeric, Text, BigInteger, SmallInteger, Integer, DateTime
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.dialects.mysql import (TINYINT as mysql_TINYINT,
                                       SMALLINT as mysql_SMALLINT,
                                       MEDIUMINT as mysql_MEDIUMINT,
                                       INTEGER as mysql_INTEGER,
                                       BIGINT as mysql_BIGINT,
                                       LONGTEXT as mysql_LONGTEXT
                                       )
from sqlalchemy import (UniqueConstraint,
                        ForeignKeyConstraint,
                        CheckConstraint,
                        MetaData,
                        PrimaryKeyConstraint,
                        func,
                        create_engine,
                        inspect)
from multiprocessing import cpu_count
import concurrent.futures as cf
import logging


# -----------------------------------------------------------------------------

logger = logging.getLogger('db_migrator')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('db_migrator.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)

# -----------------------------------------------------------------------------


def fill_table(o_engine_conn, d_engine_conn, table_name, chunk_size):
    """
    Fills existing table in dest with origin table data.
    """
    logger.info('Migrating {} table'.format(table_name))
    o_engine = create_engine(o_engine_conn)
    # sqlalchemy wrongly shortens oracle col names if
    # len(col_name) > 30 - 6. 
    # It should only do only if col_name > 30
    o_engine.dialect.max_identifier_length = 36
    o_metadata = MetaData()
    o_metadata.reflect(o_engine)
    d_engine = create_engine(d_engine_conn)
    d_metadata = MetaData()
    d_metadata.reflect(d_engine)

    table = o_metadata.tables[table_name]
    pks = [c for c in table.primary_key.columns]
    pk = pks[0]
    single_pk = True if len(pks) == 1 else False

    # Check if the table exists in migrated db, if needs to be completed and set starting pk id
    try:
        first_it = True
        d_table = d_metadata.tables[table_name]
        dpk = [c for c in d_table.primary_key.columns][0]
        count = o_engine.execute(select([func.count(pk)])).fetchone()[0]
        d_count = d_engine.execute(select([func.count(dpk)])).fetchone()[0]
        # Nothing to do here
        if count == d_count:
            logger.info(
                '{} table exists in dest and has same counts than origin table. Skipping.'
                .format(table_name))
            return True
        elif count != d_count and d_count != 0:
            q = select([d_table]).order_by(dpk.desc()).limit(1)
            res = d_engine.execute(q)
            next_id = res.fetchone().__getitem__(dpk.name)
            first_it = False
    except:
        logger.error(
            'Need to create {} table before filling it'.format(table_name))

    # table has a composite pk (usualy a bad design choice).
    # Uses offset and limit to copy data from origin to dest,
    # not v good performance.
    if not single_pk:
        if not first_it:
            offset = d_count
        else:
            offset = 0
        for ini in [x for x in range(offset, count-offset, chunk_size)]:
            # use offset and limit, terrible performance.
            q = select([table]).order_by(*pks).offset(ini).limit(chunk_size)
            res = o_engine.execute(q)
            data = res.fetchall()
            d_engine.execute(
                table.insert(),
                [
                    dict([(col_name, col_value)
                          for col_name, col_value in zip(res.keys(), row)])
                    for row in data
                ]
            )
    else:
        # table has a single pk field. Sorting by pk and paginating.
        while True:
            q = select([table]).order_by(pk).limit(chunk_size)
            if not first_it:
                q = q.where(pk > next_id)
            else:
                first_it = False
            res = o_engine.execute(q)
            data = res.fetchall()
            if len(data):
                next_id = data[-1].__getitem__(pk.name)
                d_engine.execute(
                    table.insert(),
                    [
                        dict([(col_name, col_value)
                              for col_name, col_value in zip(res.keys(), row)])
                        for row in data
                    ]
                )
            else:
                break
    logger.info('{} table filled'.format(table_name))
    return True


class DbMigrator(object):
    """Migrator class.

    Migrates origin db to dest one.

    Attributes:
        o_conn_string: Origin DB connection string.
        d_conn_string: Dest DB connection string.
        exclude: list of tables to not migrate.
        n_workers: Number of processes.

    Methods
        copy_schema: Copies tables to dest.
        copy_constraints: Copies constraints to dest.
        copy_indexes: Copies indexes to dest.
        migrate: Migrates origin to dest.
    """
    o_engine_conn = None
    d_engine_conn = None
    n_cores = None

    def __init__(self, o_conn_string, d_conn_string, exclude=[], n_workers=cpu_count()):
        self.o_engine_conn = o_conn_string
        self.d_engine_conn = d_conn_string
        self.n_cores = n_workers

        # exclude tables with no pk
        o_engine = create_engine(self.o_engine_conn)
        metadata = MetaData()
        metadata.reflect(o_engine)
        no_pk = []
        for table_name, table in metadata.tables.items():
            pks = [c for c in table.primary_key.columns]
            if not pks:
                no_pk.append(table_name)
        self.exclude = exclude + no_pk

    def __fix_column_type(self, col, db_engine):
        """
        Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        Borrowed from sqlacodegen.
        """
        cls = col.type.__class__
        for supercls in cls.__mro__:
            if hasattr(supercls, '__visit_name__'):
                cls = supercls
            if supercls.__name__ != supercls.__name__.upper() and not supercls.__name__.startswith('_'):
                break
        col.type = col.type.adapt(cls)

        # unset any server default value
        col.server_default = None

        # refine types
        if isinstance(col.type, Numeric):
            if col.type.scale == 0:
                if db_engine == 'mysql':
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
                elif db_engine in ['postgresql', 'sqlite']:
                    if not col.type.precision or col.type.precision > 4:
                        col.type = col.type.adapt(BigInteger)
                    else:
                        if col.type.precision <= 2:
                            col.type = col.type.adapt(SmallInteger)
                        elif 2 < col.type.precision <= 4:
                            col.type = col.type.adapt(Integer)
            else:
                if db_engine == 'mysql':
                    if not col.type.precision and not col.type.scale:
                        col.type.precision = 64 # max mysql precision
                        col.type.scale = 30 # max mysql scale
        elif isinstance(col.type, Text):
            if db_engine == 'mysql':
                col.type = col.type.adapt(mysql_LONGTEXT)
        return col

    def copy_schema(self):
        """        
        Copies the schema to dest db.
        Copies all constraints in sqlite, only pk in mysql and postgres.
        """
        o_engine = create_engine(self.o_engine_conn)
        d_engine = create_engine(self.d_engine_conn)
        metadata = MetaData()
        metadata.reflect(o_engine)
        insp = inspect(o_engine)

        new_metadata_tables = {}
        for table_name, table in metadata.tables.items():
            if table_name in self.exclude:
                continue
            # Keep everything for sqlite. SQLite cant alter table ADD CONSTRAINT.
            # Only 1 simultaneous process can write to it.
            # Keep only PKs for PostreSQL and MySQL.
            # Restoring them after all data is copied.
            keep_constraints = list(filter(lambda cons: isinstance(
                cons, PrimaryKeyConstraint), table.constraints))
            if d_engine.name == 'sqlite':
                uks = insp.get_unique_constraints(table_name)
                for uk in uks:
                    uk_cols = filter(
                        lambda c: c.name in uk['column_names'], table._columns)
                    keep_constraints.append(
                        UniqueConstraint(*uk_cols, name=uk['name']))
                for fk in filter(lambda cons: isinstance(cons, ForeignKeyConstraint), table.constraints):
                    keep_constraints.append(fk)
                for cc in filter(lambda cons: isinstance(cons, CheckConstraint), table.constraints):
                    cc.sqltext = TextClause(str(cc.sqltext).replace("\"", ""))
                    keep_constraints.append(cc)
                table.constraints = set(keep_constraints)
            else:
                table.constraints = set(keep_constraints)

            table.indexes = set()

            # TODO: Hacky. Fails when reflecting column/table comments so removing it.
            new_metadata_cols = ColumnCollection()
            for col in table._columns:
                col.comment = None
                col = self.__fix_column_type(col, d_engine.name)
                # be sure that no column has auto-increment
                col.autoincrement = False
                new_metadata_cols.add(col)
            table.columns = new_metadata_cols.as_immutable()
            table.comment = None
            new_metadata_tables[table_name] = table
        metadata.tables = immutabledict(new_metadata_tables)
        metadata.create_all(d_engine)

    def validate_migration(self):
        """
        Checks that counts for all tables in origin an dest dbd are equal.
        """
        o_engine = create_engine(self.o_engine_conn)
        o_metadata = MetaData()
        o_metadata.reflect(o_engine)
        d_engine = create_engine(self.d_engine_conn)
        d_metadata = MetaData()
        d_metadata.reflect(d_engine)

        o_tables = filter(
            lambda x: x[0] not in self.exclude, o_metadata.tables.items())
        d_tables = filter(
            lambda x: x[0] not in self.exclude, d_metadata.tables.items())
        o_tables = {table_name: table for table_name, table in o_tables}
        d_tables = {table_name: table for table_name, table in d_tables}

        if set(o_tables.keys()) != set(d_tables.keys()):
            return False

        validated = True
        o_s = Session(o_engine)
        d_s = Session(d_engine)
        for table_name, table in o_tables.items():
            migrated_table = d_tables[table_name]
            if o_s.query(table).count() != d_s.query(migrated_table).count():
                logger.error('Row count failed for table {}, {}, {}'
                             .format(table_name,
                                     o_s.query(
                                         table).count(),
                                     d_s.query(migrated_table).count()))

                validated = False
        o_s.close()
        d_s.close()
        return validated

    def copy_constraints(self):
        """
        Migrates constraints, UKs, CCs and FKs.
        """
        o_engine = create_engine(self.o_engine_conn)
        d_engine = create_engine(self.d_engine_conn)
        metadata = MetaData()
        metadata.reflect(o_engine)

        insp = inspect(o_engine)

        tables = filter(
            lambda x: x[0] not in self.exclude, metadata.tables.items())
        for table_name, table in tables:
            constraints_to_keep = []
            # keep unique constraints
            uks = insp.get_unique_constraints(table_name)
            for uk in uks:
                uk_cols = filter(
                    lambda c: c.name in uk['column_names'], table._columns)
                uuk = UniqueConstraint(*uk_cols, name=uk['name'])
                uuk._set_parent(table)
                constraints_to_keep.append(uuk)

            # keep check constraints
            ccs = filter(lambda cons: isinstance(
                cons, CheckConstraint), table.constraints)
            for cc in ccs:
                cc.sqltext = TextClause(str(cc.sqltext).replace("\"", ""))
                constraints_to_keep.append(cc)

            # keep fks
            for fk in filter(lambda cons: isinstance(cons, ForeignKeyConstraint), table.constraints):
                constraints_to_keep.append(fk)

            # create all constraints
            for cons in constraints_to_keep:
                try:
                    d_engine.execute(AddConstraint(cons))
                except Exception as e:
                    logger.warning(e)

    def copy_indexes(self):
        """
        Creates indexes in dest when possible.
        """
        o_engine = create_engine(self.o_engine_conn)
        d_engine = create_engine(self.d_engine_conn)
        metadata = MetaData()
        metadata.reflect(o_engine)

        insp = inspect(o_engine)

        tables = filter(
            lambda x: x[0] not in self.exclude, metadata.tables.items())
        for table_name, table in tables:
            uks = insp.get_unique_constraints(table_name)
            # UKs are internally implemented as a unique indexes.
            # Do not create index if it exists a UK for that field.
            indexes_to_keep = filter(lambda index: index.name not in [
                                     x['name'] for x in uks], table.indexes)

            for index in indexes_to_keep:
                try:
                    index.create(d_engine)
                except Exception as e:
                    logger.info(e)

    def __sort_tables(self):
        """
        Sort tables by FK dependency. 
        SQLite cannot ALTER to add constraints 
        and only supports one concurrent write process.
        """
        o_engine = create_engine(self.o_engine_conn)
        metadata = MetaData()
        metadata.reflect(o_engine)

        tables = filter(
            lambda x: x[0] not in self.exclude, metadata.tables.items())
        tables = [x[1] for x in tables]

        ordered_tables = []
        # sort tables by fk dependency, required for sqlite
        while tables:
            current_table = tables[0]

            all_fk_done = True
            for fk in filter(lambda x: isinstance(x, ForeignKeyConstraint), current_table.constraints):
                if fk.referred_table.name not in ordered_tables:
                    all_fk_done = False
            if all_fk_done:
                ordered_tables.append(current_table.name)
                # delete first element of the list(current table) after data is copied
                del tables[0]
            else:
                # put current table in last position of the list
                tables.append(tables.pop(0))
        return ordered_tables

    def migrate(self, copy_schema=True, copy_data=True, copy_constraints=True, copy_indexes=True, chunk_size=1000):
        """
        Copies origin database to dest.
        """
        d_engine = create_engine(self.d_engine_conn)

        # copy tables from origin to dest
        if copy_schema:
            self.copy_schema()

        # fill tables in dest
        if copy_data:
            tables = self.__sort_tables()
            # SQLite accepts concurrent read but not write
            processes = 1 if d_engine.name == 'sqlite' else self.n_cores

            with cf.ProcessPoolExecutor(max_workers=processes) as exe:
                futures = {exe.submit(fill_table, self.o_engine_conn, self.d_engine_conn, table, chunk_size):
                           table for table in tables}

                for future in cf.as_completed(futures):
                    table = futures[future]
                    try:
                        res = future.result()
                        if not res:
                            logger.error(
                                'Something went wrong when copying table {}: '.format(table))
                    except Exception as e:
                        logger.error('Table {} worker died: '.format(table), e)

        # check row counts for each table
        if not copy_data:
            all_migrated = True
        else:
            all_migrated = self.validate_migration()

        # copy constraints and indexes
        if all_migrated:
            logger.info(
                'All tables succesfuly migrated')
            # do not migrate constraints in sqlite, we initially kept all of them as
            # it does not support alter table ADD CONSTRAINT.
            if copy_constraints and d_engine.name != 'sqlite':
                self.copy_constraints()
            if copy_indexes:
                self.copy_indexes()
            logger.info(
                'Migration succesfuly finished')
        else:
            logger.error(
                'Table migration did not pass the validation, constraints and indexes not copied across')
        return all_migrated
