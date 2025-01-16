[![CI Testing](https://github.com/chembl/cbl_migrator/workflows/CI/badge.svg)](https://github.com/chembl/cbl_migrator/actions?query=workflow%3ACI+branch%3Amaster)
[![Latest Version](https://img.shields.io/pypi/v/cbl_migrator.svg)](https://pypi.python.org/pypi/cbl_migrator/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/cbl_migrator.svg)](https://pypi.python.org/pypi/cbl_migrator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# CBL Migrator

A lightweight SQLAlchemy-based tool that migrates Oracle databases to MySQL, PostgreSQL, or SQLite. It is used in ChEMBL dump generation.

## Usage in Python
```python
from cbl_migrator import DbMigrator

origin = 'oracle://{user}:{pass}@{host}:{port}/?service_name={service_name}&encoding=utf8'
dest = 'postgresql://{user}:{pass}@{host}:{port}/{dbname}?client_encoding=utf8'

migrator = DbMigrator(origin, dest, ['excluded_table1', 'excluded_table2'], n_workers=4)
migrator.migrate()
```

## Command Line Usage
```bash
cbl-migrator "oracle://{user}:{pass}@{host}:{port}/?service_name={service_name}&encoding=utf8" \
             "postgresql://{user}:{pass}@{host}:{port}/{dbname}?client_encoding=utf8" \
             --n_workers 8
```

## How It Works
- Copies tables from the source, preserving only PKs initially.  
- Migrates table data in parallel.  
- If successful, applies constraints and then indexes; skips indexes already covered by unique keys.  
- Logs objects that fail to migrate.

## What It Does Not Do
- Avoids tables without PKs (may hang if a unique field is referenced by an FK).  
- Ignores server default values, autoincrement fields, triggers, and procedures.

## SQLite
- No concurrent writes or ALTER TABLE ADD CONSTRAINT.  
- Uses one core and creates constraints at table creation time.  
- Inserts rows sequentially in correct FK order.

## MySQL
- Converts CLOBs to LONGTEXT.
