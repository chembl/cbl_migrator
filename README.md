# DB Migrator

Small library that migrates Oracle DBs to MySQL, PostgreSQL and SQLite. Used in ChEMBL dumps generation process.

Copies the schema, tables rows, constraints and indexes from Oracle to another RDBMS.

to use it:

```python
from db_migrator import DbMigrator

origin = 'oracle://{user}:{pass}@{host}:{port}/{sid}'
dest = 'postgresql://{user}:{pass}@{host}:{port}/{dbname}'

migrator = DbMigrator(origin, dest, ['excluded_table1', 'excluded_table2'])
migrator.migrate()
```

## What it does (in order of events)

- Copies tables from origin to dest using the closest data type for each field. No constraints except PK are initially copied across.
- Data is migrated from origin to dest tables. In parallel.
- If the data migration is succesful, it generates first the constraints and then the indexes in dest table. Any index in a field with a previous created UK will be skipped (UKs are implemented as unique indexes).
- It logs every time it was not possible to migrate an object. Ex: 

## What it does not do

- It won't migrate any table without a defined PK. May hang with a table with no PK but a field with a UK being pointed as FK for another table.
- It does not try to migrate server default values.
- It does not set auto fields.

## SQLite

Sqlite can not:

- concurrently write
- alter table ADD CONSTRAINT

So only one core is used when migrating to it. All constraints are generated at the time of generating the destination tables and it sequentially inserts rows in tables in correct FKs order.
