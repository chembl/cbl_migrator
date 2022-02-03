from sqlalchemy.util._collections import immutabledict
from sqlalchemy.sql.base import ColumnCollection
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.schema import AddConstraint
from sqlalchemy.sql import select
from sqlalchemy import (
    UniqueConstraint,
    ForeignKeyConstraint,
    CheckConstraint,
    MetaData,
    PrimaryKeyConstraint,
    func,
    create_engine,
    inspect,
)
import concurrent.futures as cf
import os
from .conv import COLTYPE_CONV
from .logs import logger


def fill_table(o_eng_conn, d_eng_conn, table_name, chunk_size):
    """
    Fills existing table in dest with origin table data.
    """
    logger.info(f"Migrating {table_name} table")
    d_eng = create_engine(d_eng_conn, future=True)
    d_metadata = MetaData()
    d_metadata.reflect(d_eng)

    o_eng = create_engine(o_eng_conn, future=True)
    if d_eng.name == "mysql":
        # https://github.com/sqlalchemy/sqlalchemy/blob/ed78e679eafe787f4c152b78726bf1e1b91ab465/lib/sqlalchemy/dialects/mysql/base.py#L2332
        o_eng.dialect.max_identifier_length = 64
    if o_eng.dialect.max_identifier_length > d_eng.dialect.max_identifier_length:
        o_eng.dialect.max_identifier_length = d_eng.dialect.max_identifier_length

    o_metadata = MetaData()
    o_metadata.reflect(o_eng)

    table = o_metadata.tables[table_name]
    pks = [c for c in table.primary_key.columns]
    pk = pks[0]
    single_pk = True if len(pks) == 1 else False

    # Check if the table exists in migrated db, if needs to be completed and set starting pk id
    try:
        d_table = d_metadata.tables[table_name]
    except Exception as e:
        logger.error(f"Need to create {table_name} table before filling it", e)
        raise Exception(f"Need to create {table_name} table before filling it")

    dpk = [c for c in d_table.primary_key.columns][0]
    with o_eng.connect() as conn:
        count = conn.execute(select(func.count(pk))).scalar()
    with d_eng.connect() as conn:
        d_count = conn.execute(select(func.count(dpk))).scalar()
    first_it = True
    if count == d_count:
        logger.info(
            f"{table_name} table exists in dest and has same counts than origin table. Skipping."
        )
        return True
    elif count != d_count and d_count != 0:
        q = select(d_table).order_by(dpk.desc()).limit(1)
        with d_eng.connect() as conn:
            res = conn.execute(q)
            last_id = res.first().__getitem__(dpk.name)
        first_it = False

    # table has a composite pk (usualy a bad design choice).
    # Uses offset and limit to copy data from origin to dest,
    # not v good performance.
    if not single_pk:
        if not first_it:
            offset = d_count
        else:
            offset = 0
        for ini in [x for x in range(offset, count - offset, chunk_size)]:
            q = select(table).order_by(*pks).offset(ini).limit(chunk_size)
            with o_eng.connect() as connr:
                res = connr.execute(q)
                data = res.all()
                with d_eng.begin() as conn:
                    conn.execute(
                        table.insert(),
                        [
                            dict(
                                [
                                    (col_name, col_value)
                                    for col_name, col_value in zip(res.keys(), row)
                                ]
                            )
                            for row in data
                        ],
                    )
    else:
        # table has a single pk field. Sorting by pk and paginating.
        while True:
            q = select(table).order_by(pk).limit(chunk_size)
            if not first_it:
                q = q.where(pk > last_id)
            else:
                first_it = False
            with o_eng.connect() as connr:
                res = connr.execute(q)
                data = res.all()
                if len(data):
                    last_id = data[-1].__getitem__(pk.name)
                    with d_eng.begin() as conn:
                        conn.execute(
                            table.insert(),
                            [
                                dict(
                                    [
                                        (col_name, col_value)
                                        for col_name, col_value in zip(res.keys(), row)
                                    ]
                                )
                                for row in data
                            ],
                        )
                else:
                    break
    logger.info(f"{table_name} table filled")
    return True


class DbMigrator:
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

    def __init__(self, o_conn_string, d_conn_string, exclude=[], n_workers=4):
        self.o_eng_conn = o_conn_string
        self.d_eng_conn = d_conn_string
        self.n_cores = n_workers

        o_eng = create_engine(self.o_eng_conn, future=True)
        metadata = MetaData()
        metadata.reflect(o_eng)
        no_pk = []
        for table_name, table in metadata.tables.items():
            pks = [c for c in table.primary_key.columns]
            if not pks:
                no_pk.append(table_name)
        # exclude tables with no pk
        self.exclude = exclude + no_pk

    def __fix_column_type(self, col, o_eng, d_eng):
        """
        Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        """
        # bit borrowed from sqlacodegen.
        for supercls in col.type.__class__.__mro__:
            if not supercls.__name__.startswith("_") and hasattr(
                supercls, "__visit_name__"
            ):
                try:
                    col.type = col.type.adapt(supercls)
                except TypeError:
                    break

        # unset any server default value
        col.server_default = None

        if o_eng in COLTYPE_CONV and d_eng in COLTYPE_CONV[o_eng]:
            col.type = COLTYPE_CONV[o_eng][d_eng](col.type)
        else:
            raise Exception(f"Migration from {o_eng} to {d_eng} not available")
        return col

    def __copy_schema(self):
        """        
        Copies the schema to dest db.
        Copies all constraints in sqlite, only pk in mysql and postgres.
        """
        o_eng = create_engine(self.o_eng_conn, future=True)
        d_eng = create_engine(self.d_eng_conn, future=True)
        metadata = MetaData()
        metadata.reflect(o_eng)
        insp = inspect(o_eng)

        new_metadata_tables = {}
        tables = filter(lambda x: x[0] not in self.exclude, metadata.tables.items())
        for table_name, table in tables:
            keep_constraints = list(
                filter(
                    lambda cons: isinstance(cons, PrimaryKeyConstraint),
                    table.constraints,
                )
            )
            if d_eng.name == "sqlite":
                # Keep everything for sqlite. SQLite cant alter table ADD CONSTRAINT.
                uks = insp.get_unique_constraints(table_name)
                for uk in uks:
                    uk_cols = filter(
                        lambda c: c.name in uk["column_names"], table._columns
                    )
                    keep_constraints.append(UniqueConstraint(*uk_cols, name=uk["name"]))
                for fk in filter(
                    lambda cons: isinstance(cons, ForeignKeyConstraint),
                    table.constraints,
                ):
                    keep_constraints.append(fk)
                for cc in filter(
                    lambda cons: isinstance(cons, CheckConstraint), table.constraints
                ):
                    cc.sqltext = TextClause(str(cc.sqltext).replace('"', ""))
                    keep_constraints.append(cc)
                table.constraints = set(keep_constraints)
            else:
                table.constraints = set(keep_constraints)

            table.indexes = set()

            new_metadata_cols = ColumnCollection()
            for col in table._columns:
                col = self.__fix_column_type(col, o_eng.name, d_eng.name)
                col.autoincrement = False
                new_metadata_cols.add(col)
            table.columns = new_metadata_cols.as_immutable()
            new_metadata_tables[table_name] = table
        metadata.tables = immutabledict(new_metadata_tables)
        metadata.create_all(d_eng)

    def validate_migration(self):
        """
        Checks that counts for all tables in origin an dest dbs are equal.
        """
        o_eng = create_engine(self.o_eng_conn, future=True)
        o_metadata = MetaData()
        o_metadata.reflect(o_eng)
        d_eng = create_engine(self.d_eng_conn, future=True)
        d_metadata = MetaData()
        d_metadata.reflect(d_eng)

        o_tables = filter(lambda x: x[0] not in self.exclude, o_metadata.tables.items())
        d_tables = filter(lambda x: x[0] not in self.exclude, d_metadata.tables.items())
        o_tables = {table_name: table for table_name, table in o_tables}
        d_tables = {table_name: table for table_name, table in d_tables}

        if set(o_tables.keys()) != set(d_tables.keys()):
            return False

        validated = True
        with o_eng.connect() as o_s:
            with d_eng.connect() as d_s:
                for table_name, table in o_tables.items():
                    migrated_table = d_tables[table_name]
                    o_count = o_s.execute(
                        select(func.count()).select_from(table)
                    ).scalar()
                    d_count = d_s.execute(
                        select(func.count()).select_from(migrated_table)
                    ).scalar()
                    if o_count != d_count:
                        logger.error(
                            f"Row count failed for table {table_name}, {o_count}, {d_count}"
                        )
                        validated = False
        return validated

    def __copy_constraints(self):
        """
        Migrates constraints, UKs, CCs and FKs.
        """
        o_eng = create_engine(self.o_eng_conn, future=True)
        d_eng = create_engine(self.d_eng_conn, future=True)
        metadata = MetaData()
        metadata.reflect(o_eng)

        insp = inspect(o_eng)

        tables = filter(lambda x: x[0] not in self.exclude, metadata.tables.items())
        for table_name, table in tables:
            constraints_to_keep = []
            # keep unique constraints
            uks = insp.get_unique_constraints(table_name)
            for uk in uks:
                uk_cols = filter(lambda c: c.name in uk["column_names"], table._columns)
                uuk = UniqueConstraint(*uk_cols, name=uk["name"])
                uuk._set_parent(table)
                constraints_to_keep.append(uuk)

            # keep check constraints
            ccs = filter(
                lambda cons: isinstance(cons, CheckConstraint), table.constraints
            )
            for cc in ccs:
                cc.sqltext = TextClause(str(cc.sqltext).replace('"', ""))
                constraints_to_keep.append(cc)

            # keep fks
            for fk in filter(
                lambda cons: isinstance(cons, ForeignKeyConstraint), table.constraints
            ):
                constraints_to_keep.append(fk)

            # create all constraints
            for cons in constraints_to_keep:
                try:
                    with d_eng.begin() as conn:
                        conn.execute(AddConstraint(cons))
                except Exception as e:
                    logger.warning(e)

    def __copy_indexes(self):
        """
        Creates indexes in dest when possible.
        """
        o_eng = create_engine(self.o_eng_conn, future=True)
        d_eng = create_engine(self.d_eng_conn, future=True)
        metadata = MetaData()
        metadata.reflect(o_eng)

        insp = inspect(o_eng)

        tables = filter(lambda x: x[0] not in self.exclude, metadata.tables.items())
        for table_name, table in tables:
            uks = insp.get_unique_constraints(table_name)
            # UKs are internally implemented as a unique indexes.
            # Do not create index if it exists a UK for that field.
            indexes_to_keep = filter(
                lambda index: index.name not in [x["name"] for x in uks], table.indexes
            )

            for index in indexes_to_keep:
                try:
                    index.create(d_eng)
                except Exception as e:
                    logger.warning(e)

    def migrate(
        self,
        copy_schema=True,
        copy_data=True,
        copy_constraints=True,
        copy_indexes=True,
        chunk_size=1000,
    ):
        """migrate

        executes the migration.

        Args:
            copy_schema: Bool. False won't create tables in dest.
            copy_data: Bool. False to generate empty tables.
            copy_constraints: Bool. False won't create UKs, FKs nor CKs in dest.
            copy_indexes: Bool. False won't create indexes in dest.
            chunk_size: Number of records copied in each chunk.
        """
        o_eng = create_engine(self.o_eng_conn, future=True)
        d_eng = create_engine(self.d_eng_conn, future=True)

        if o_eng.dialect.max_identifier_length > d_eng.dialect.max_identifier_length:
            logger.info(f"{o_eng.name} max_identifier_length larger than {d_eng.name}")

        if o_eng.name == "sqlite":
            sqlite_db_path = o_eng.url.database
            if not sqlite_db_path or (
                not os.path.isfile(sqlite_db_path)
                or os.path.getsize(sqlite_db_path) < 100
            ):
                raise Exception("Origin SQLite database doesn't exist or is too small")

        # copy tables from origin to dest
        if copy_schema:
            logger.info("Copying schema")
            self.__copy_schema()
            logger.info("Schema copied")

        # fill tables in dest
        if copy_data:
            metadata = MetaData()
            metadata.reflect(o_eng)
            insp = inspect(o_eng)
            tables = [
                table[0]
                for table in insp.get_sorted_table_and_fkc_names()
                if table[0] and table[0] not in self.exclude
            ]

            # SQLite accepts concurrent read but not write
            processes = 1 if d_eng.name == "sqlite" else self.n_cores
            o_eng.dispose()
            d_eng.dispose()
            if processes == 1:
                for table in tables:
                    fill_table(self.o_eng_conn, self.d_eng_conn, table, chunk_size)
            else:
                with cf.ProcessPoolExecutor(max_workers=processes) as exe:
                    futures = {
                        exe.submit(
                            fill_table,
                            self.o_eng_conn,
                            self.d_eng_conn,
                            table,
                            chunk_size,
                        ): table
                        for table in tables
                    }

                    for future in cf.as_completed(futures):
                        table = futures[future]
                        try:
                            res = future.result()
                            if not res:
                                logger.error(
                                    f"Something went wrong when copying table: {table}"
                                )
                        except Exception as e:
                            logger.error(f"Table {table} worker died: ", e)

        # check row counts for each table
        if not copy_data:
            all_migrated = True
        else:
            all_migrated = self.validate_migration()

        # copy constraints and indexes
        if all_migrated:
            logger.info("All tables succesfuly migrated")
            # do not migrate constraints in sqlite, we initially kept all of them as
            # it does not support alter table ADD CONSTRAINT.
            if copy_constraints and d_eng.name != "sqlite":
                logger.info("Copying constraints")
                self.__copy_constraints()
                logger.info("Constraints created")
            if copy_indexes:
                logger.info("Copying indexes")
                self.__copy_indexes()
                logger.info("Indexes created")
            logger.info("Migration completed")
        else:
            logger.error(
                "Table migration did not pass the validation, constraints and indexes not copied across"
            )
        return all_migrated
