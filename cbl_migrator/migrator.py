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


def chunked_copy_single_pk(table, pk, last_id, chunk_size, o_eng, d_eng):
    """
    Copies table data in chunks, assuming a single PK column.
    """
    first_it = True if last_id is None else False
    while True:
        q = select(table).order_by(pk).limit(chunk_size)
        if not first_it:
            q = q.where(pk > last_id)
        else:
            first_it = False
        with o_eng.connect() as connr:
            res = connr.execute(q)
            data = res.all()
            if data:
                last_id = getattr(data[-1], pk.name)
                with d_eng.begin() as conn:
                    conn.execute(
                        table.insert(),
                        [dict(zip(res.keys(), row)) for row in data],
                    )
            else:
                break


def chunked_copy_multi_pk(table, pks, count, offset, chunk_size, o_eng, d_eng):
    """
    Copies table data in chunks, assuming a composite PK.
    """
    for ini in range(offset, count - offset, chunk_size):
        q = select(table).order_by(*pks).offset(ini).limit(chunk_size)
        with o_eng.connect() as connr:
            res = connr.execute(q)
            data = res.all()
            with d_eng.begin() as conn:
                conn.execute(
                    table.insert(),
                    [dict(zip(res.keys(), row)) for row in data],
                )


def fill_table(o_eng_conn, d_eng_conn, table, chunk_size):
    """
    Fills existing table in the destination with data from the origin.
    Skips if destination already has the same row count.
    Makes partial reads/writes depending on PK presence.
    """
    logger.info(f"Starting migration of table '{table.name}'")
    d_eng = create_engine(d_eng_conn)
    o_eng = create_engine(o_eng_conn)

    # Adjust identifier length if necessary
    if d_eng.name == "mysql":
        o_eng.dialect.max_identifier_length = 64
    if o_eng.dialect.max_identifier_length > d_eng.dialect.max_identifier_length:
        logger.warning(
            f"Adjusting identifier length from {o_eng.dialect.max_identifier_length} "
            f"to {d_eng.dialect.max_identifier_length}"
        )
        o_eng.dialect.max_identifier_length = d_eng.dialect.max_identifier_length

    pks = [c for c in table.primary_key.columns]
    single_pk = len(pks) == 1
    pk = pks[0] if pks else None

    # Row count checks
    with o_eng.connect() as conn:
        count = conn.execute(select(func.count(pk))).scalar()
    with d_eng.connect() as conn:
        try:
            d_count = conn.execute(select(func.count(pk))).scalar()
        except Exception as e:
            logger.error(f"Need to create {table.name} table before filling it", e)
            raise

    if count == d_count:
        logger.info(
            f"Table '{table.name}' already matches origin row count ({count} rows). Skipping."
        )
        return True
    elif count != d_count and d_count != 0:
        logger.info(f"Resuming migration of '{table.name}' from last ID")
        q = select(pk).order_by(pk.desc()).limit(1)
        with d_eng.connect() as conn:
            last_id = conn.scalar(q)
    else:
        logger.info(f"Starting fresh migration of '{table.name}' ({count} rows)")
        last_id = None

    # Multi or single PK copy
    if single_pk:
        chunked_copy_single_pk(table, pk, last_id, chunk_size, o_eng, d_eng)
    else:
        offset = d_count if last_id else 0
        chunked_copy_multi_pk(table, pks, count, offset, chunk_size, o_eng, d_eng)

    logger.info(f"Successfully completed migration of table '{table.name}'")
    return True


class DbMigrator:
    """
    Handles database migrations from an origin DB to a destination DB.

    Attributes:
        o_conn_string (str): Origin DB connection string.
        d_conn_string (str): Destination DB connection string.
        exclude (list[str]): List of tables to exclude from migration.
        exclude_fields (list[str]): List of fields to exclude in format 'table.field'.
        n_cores (int): Number of processes used for data copying.
    """

    def __init__(
        self,
        o_conn_string,
        d_conn_string,
        exclude_tables=None,
        exclude_fields=None,
        n_workers=4,
    ):
        if exclude_tables is None:
            exclude_tables = []
        if exclude_fields is None:
            exclude_fields = []
        self.o_eng_conn = o_conn_string
        self.d_eng_conn = d_conn_string
        self.n_cores = n_workers
        self.exclude_fields = {}
        for item in exclude_fields:
            table, field = item.lower().split(".")
            if table not in self.exclude_fields:
                self.exclude_fields[table] = []
            self.exclude_fields[table].append(field)

        o_eng = create_engine(self.o_eng_conn)
        metadata = MetaData()
        metadata.reflect(o_eng)
        no_pk = [
            table_name.lower()
            for table_name, table in metadata.tables.items()
            if not list(table.primary_key.columns)
        ]
        self.exclude_tables = exclude_tables + no_pk

    def __fix_column_type(self, col, o_eng, d_eng):
        """
        Adapts column types to generic types and unsets server defaults.
        """
        cls = col.type.__class__
        for supercls in cls.__mro__:
            if hasattr(supercls, "__visit_name__"):
                cls = supercls
            if (
                supercls.__name__ != supercls.__name__.upper()
                and not supercls.__name__.startswith("_")
            ):
                break
        col.type = col.type.adapt(cls)
        col.server_default = None

        if o_eng in COLTYPE_CONV and d_eng in COLTYPE_CONV[o_eng]:
            col = COLTYPE_CONV[o_eng][d_eng](col)
        else:
            raise Exception(f"Migration from {o_eng} to {d_eng} not available")
        return col

    def __copy_schema(self):
        """
        Copies schema from origin to destination, preserving PKs,
        and optionally other constraints if destination is SQLite.
        """
        o_eng = create_engine(self.o_eng_conn)
        d_eng = create_engine(self.d_eng_conn)
        metadata = MetaData()
        metadata.reflect(o_eng)
        insp = inspect(o_eng)

        new_metadata_tables = {}
        # Filter tables, excluding those in self.exclude_tables
        tables = [
            (name, table)
            for name, table in metadata.tables.items()
            if name.lower() not in self.exclude_tables
        ]

        for table_name, table in tables:
            # Keep only PK constraints unless it's SQLite
            keep_constraints = [
                cons
                for cons in table.constraints
                if isinstance(cons, PrimaryKeyConstraint)
            ]
            if d_eng.name == "sqlite":
                # Retain all constraints for SQLite except those involving excluded fields
                excluded_fields = self.exclude_fields.get(table_name.lower(), [])

                # Unique constraints
                uks = insp.get_unique_constraints(table_name)
                for uk in uks:
                    if not any(
                        col.lower() in excluded_fields for col in uk["column_names"]
                    ):
                        uk_cols = [
                            c for c in table._columns if c.name in uk["column_names"]
                        ]
                        keep_constraints.append(
                            UniqueConstraint(*uk_cols, name=uk["name"])
                        )

                # Foreign key constraints
                for fk in [
                    cons
                    for cons in table.constraints
                    if isinstance(cons, ForeignKeyConstraint)
                ]:
                    if not any(
                        col.name.lower() in excluded_fields for col in fk.columns
                    ):
                        keep_constraints.append(fk)

                # Check constraints
                for cc in [
                    cons
                    for cons in table.constraints
                    if isinstance(cons, CheckConstraint)
                ]:
                    if not any(
                        col in str(cc.sqltext).lower() for col in excluded_fields
                    ):
                        cc.sqltext = TextClause(str(cc.sqltext).replace('"', ""))
                        keep_constraints.append(cc)
            table.constraints = set(keep_constraints)
            table.indexes = set()

            new_metadata_cols = ColumnCollection()
            excluded_fields = self.exclude_fields.get(table_name.lower(), [])
            for col in table._columns:
                if col.name.lower() not in excluded_fields:
                    col = self.__fix_column_type(col, o_eng.name, d_eng.name)
                    col.autoincrement = False
                    new_metadata_cols.add(col)
            table.columns = new_metadata_cols.as_readonly()
            new_metadata_tables[table_name] = table

        metadata.tables = immutabledict(new_metadata_tables)
        metadata.create_all(d_eng)

    def validate_migration(self):
        """
        Checks row counts for all tables in both origin and destination
        to confirm migration success.
        """
        o_eng = create_engine(self.o_eng_conn)
        o_metadata = MetaData()
        o_metadata.reflect(o_eng)
        d_eng = create_engine(self.d_eng_conn)
        d_metadata = MetaData()
        d_metadata.reflect(d_eng)

        # Get origin tables, excluding those in exclude_tables
        o_tables = {}
        for table_name, table in o_metadata.tables.items():
            if table_name.lower() not in self.exclude_tables:
                o_tables[table_name] = table

        # Get destination tables, excluding those in exclude_tables
        d_tables = {}
        for table_name, table in d_metadata.tables.items():
            if table_name.lower() not in self.exclude_tables:
                d_tables[table_name] = table

        if set(o_tables.keys()) != set(d_tables.keys()):
            return False

        validated = True
        with o_eng.connect() as o_s, d_eng.connect() as d_s:
            for table_name, table in o_tables.items():
                migrated_table = d_tables[table_name]
                o_count = o_s.execute(select(func.count()).select_from(table)).scalar()
                d_count = d_s.execute(
                    select(func.count()).select_from(migrated_table)
                ).scalar()
                if o_count != d_count:
                    logger.error(
                        f"Row count mismatch for {table_name}: {o_count} vs {d_count}"
                    )
                    validated = False
        return validated

    def __copy_constraints(self):
        """
        Migrates constraints to the destination DB (UK, CK, FK), skipping those
        that involve excluded fields.
        """
        o_eng = create_engine(self.o_eng_conn)
        d_eng = create_engine(self.d_eng_conn)
        metadata = MetaData()
        metadata.reflect(o_eng)
        insp = inspect(o_eng)

        # Filter tables, excluding those in self.exclude_tables
        tables = [
            (name, table)
            for name, table in metadata.tables.items()
            if name.lower() not in self.exclude_tables
        ]

        for table_name, table in tables:
            constraints_to_keep = []
            excluded_fields = self.exclude_fields.get(table_name.lower(), [])

            # Unique constraints - skip if any column is excluded
            uks = insp.get_unique_constraints(table_name)
            for uk in uks:
                if not any(
                    col.lower() in excluded_fields for col in uk["column_names"]
                ):
                    uk_cols = [
                        c for c in table._columns if c.name in uk["column_names"]
                    ]
                    uuk = UniqueConstraint(*uk_cols, name=uk["name"])
                    uuk._set_parent(table)
                    constraints_to_keep.append(uuk)

            # Check constraints - skip if any column is excluded
            ccs = [
                cons for cons in table.constraints if isinstance(cons, CheckConstraint)
            ]
            for cc in ccs:
                if not any(col in str(cc.sqltext).lower() for col in excluded_fields):
                    cc.sqltext = TextClause(str(cc.sqltext).replace('"', ""))
                    constraints_to_keep.append(cc)

            # Foreign keys - skip if any column is excluded
            fks = [
                cons
                for cons in table.constraints
                if isinstance(cons, ForeignKeyConstraint)
            ]
            for fk in fks:
                if not any(col.name.lower() in excluded_fields for col in fk.columns):
                    constraints_to_keep.append(fk)

            # Create constraints
            for cons in constraints_to_keep:
                try:
                    with d_eng.begin() as conn:
                        conn.execute(AddConstraint(cons))
                except Exception as e:
                    logger.warning(e)

    def __copy_indexes(self):
        """
        Creates indexes in the destination DB, skipping those
        already defined via unique or primary constraints and
        those involving excluded fields.
        """
        o_eng = create_engine(self.o_eng_conn)
        d_eng = create_engine(self.d_eng_conn)
        metadata = MetaData()
        metadata.reflect(o_eng)
        insp = inspect(o_eng)

        # Filter tables, excluding those in self.exclude_tables
        tables = [
            (name, table)
            for name, table in metadata.tables.items()
            if name.lower() not in self.exclude_tables
        ]

        for table_name, table in tables:
            excluded_fields = self.exclude_fields.get(table_name.lower(), [])
            uks = insp.get_unique_constraints(table_name)
            pk = insp.get_pk_constraint(table_name)

            indexes_to_keep = [
                idx
                for idx in table.indexes
                if idx.name not in [u["name"] for u in uks]
                and idx.name != pk["name"]
                and not any(col.name.lower() in excluded_fields for col in idx.columns)
            ]
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
        """
        Orchestrates the migration from the origin DB to the destination DB.

        Args:
            copy_schema (bool): Create tables in destination from origin's schema.
            copy_data (bool): Copy table data.
            copy_constraints (bool): Migrate constraints to destination.
            copy_indexes (bool): Migrate indexes to destination.
            chunk_size (int): Batch size for chunked copying.
        """
        logger.info(
            f"Starting database migration with settings: schema={copy_schema}, "
            f"data={copy_data}, constraints={copy_constraints}, "
            f"indexes={copy_indexes}, chunk_size={chunk_size}"
        )

        o_eng = create_engine(self.o_eng_conn)
        d_eng = create_engine(self.d_eng_conn)

        if o_eng.dialect.max_identifier_length > d_eng.dialect.max_identifier_length:
            logger.info(f"{o_eng.name} max_identifier_length larger than {d_eng.name}")

        # Basic SQLite checks
        if o_eng.name == "sqlite":
            sqlite_db_path = o_eng.url.database
            if (
                not sqlite_db_path
                or not os.path.isfile(sqlite_db_path)
                or os.path.getsize(sqlite_db_path) < 100
            ):
                raise Exception("Origin SQLite database doesn't exist or is too small")

        if copy_schema:
            logger.info("Starting schema copy")
            self.__copy_schema()
            logger.info("Schema copy completed successfully")

        # Fill tables with data
        if copy_data:
            metadata = MetaData()
            metadata.reflect(d_eng)
            insp = inspect(d_eng)
            # Get all table names excluding the ones in self.exclude_tables
            all_tables_and_fks = insp.get_sorted_table_and_fkc_names()
            table_names = [
                table_name
                for table_name, _ in all_tables_and_fks
                if table_name and table_name.lower() not in self.exclude_tables
            ]

            tables = [metadata.tables[t] for t in table_names]

            processes = 1 if d_eng.name == "sqlite" else self.n_cores

            logger.info(f"Starting data migration using {processes} processes")

            with cf.ProcessPoolExecutor(max_workers=processes) as exe:
                futures = {
                    exe.submit(
                        fill_table, self.o_eng_conn, self.d_eng_conn, table, chunk_size
                    ): table
                    for table in tables
                }
                for future in cf.as_completed(futures):
                    tbl = futures[future]
                    try:
                        res = future.result()
                        if not res:
                            logger.error(f"Error copying table: {tbl}")
                    except Exception as e:
                        logger.error(f"Table {tbl} worker died: {e}")

        # Validate row counts
        all_migrated = not copy_data or self.validate_migration()

        if all_migrated:
            logger.info("Row count validation successful")
            if copy_constraints and d_eng.name != "sqlite":
                logger.info("Starting constraint migration")
                self.__copy_constraints()
                logger.info("Constraint migration completed")
            if copy_indexes:
                logger.info("Starting index migration")
                self.__copy_indexes()
                logger.info("Index migration completed")
            logger.info("Database migration completed successfully")
        else:
            logger.error("Migration failed: row count validation unsuccessful")
        return all_migrated
