# DB Migrator

Small library that migrates Oracle DBs to MySQL, PostgreSQL and SQLite. Used in ChEMBL dumps generation.

Copies the schema, tables content, constraints and indexes from Oracle to another RDBMS.
It WON'T MIGRATE any table without a defined PK. May hang if you have a table with no PK but a field with a UK being pointed as FK for another table.
It WON'T MIGRATE server defaults.

to use it:

```
from db_migrator import DbMigrator

origin = 'oracle://{user}:{pass}@{host}:{port}/{sid}'
dest = 'postgresql://{user}:{pass}@{host}:{port}/{dbname}'

migrator = DbMigrator(origin, dest)
migrator.migrate()

```


## Concurrency

Sqlite can't:

- concurrently write
- alter table ADD CONSTRAINT

So only one core is used when migrating to it. All constraints are generated at the time of generating the destination schema so it sequentially inserts in tables in correct FKs order.


As MySQL and PostgreSQL can do both things, only PKs are generated when migrating the schema.
Tables are safely filled in parallel without FK constraints. After tables are filled all constraints are restored.


## Incompatibilities

DB Migrator always tries to find the best conversion type and it also creates all existing constraints/indexes when it is possible.

If due to any RDBMS difference (ex: limit in characters to create an index) an object cannot be recreated in the destination RDBMS, the object will be skipped and the issue logged.
