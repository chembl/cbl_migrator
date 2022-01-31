[![Build Status](https://travis-ci.org/chembl/cbl_migrator.svg?branch=master)](https://travis-ci.org/chembl/cbl_migrator)
[![Latest Version](https://img.shields.io/pypi/v/cbl_migrator.svg)](https://pypi.python.org/pypi/cbl_migrator/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/cbl_migrator.svg)](https://pypi.python.org/pypi/cbl_migrator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


# CBL Migrator

Small SQLAlchemy based library that migrates Oracle DBs to MySQL, PostgreSQL and SQLite. Used in ChEMBL dumps generation process.

to use it, as a Python library:

```python
from cbl_migrator import DbMigrator

origin = 'oracle://{user}:{pass}@{host}:{port}/?service_name={service_name}?encoding=utf8'
#dest = 'mysql://{user}:{pass}@{host}:{port}/{dbname}?use_unicode=1&charset=utf8'
dest = 'postgresql://{user}:{pass}@{host}:{port}/{dbname}?client_encoding=utf8'

migrator = DbMigrator(origin, dest, ['excluded_table1', 'excluded_table2'], n_workers=4)
migrator.migrate()
```

directly from the command line:
```bash
cbl-migrator oracle://{user}:{pass}@{host}:{port}/{sid}?encoding=utf8 postgresql://{user}:{pass}@{host}:{port}/{dbname}?client_encoding=utf8 --n_workers 4 
```

## What it does (in order of events)

- Copies tables from origin to dest using the closest data type for each field. No constraints except PKs are initially copied across.
- Table contents are migrated from origin to dest tables. In parallel.
- If the data migration is succesful it will first generate the constraints and then the indexes. Any index in a field with a previously created UK will be skipped (UKs are implemented as unique indexes).
- It logs every time it was not possible to migrate an object, e.g., ```(psycopg2.OperationalError) index row size 2856 exceeds maximum 2712 for index.```

## What it does not do

- It won't migrate any table without a PK. May hang with a table without PK and containing an UK field referenced as FK in another table.
- It does not try to migrate server default values.
- It does not set autoincremental fields.
- It does not try to migrate triggers nor procedures.

## SQLite

SQLite can not:

- concurrently write
- alter table ADD CONSTRAINT

So only one core is used when migrating to it. All constraints are generated at the time of generating the destination tables and it sequentially inserts rows in tables in correct FKs order.


## MySQL

CLOBs are migrated to LONGTEXT.
