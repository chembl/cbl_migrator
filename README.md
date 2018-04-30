Package to migrate Oracle DBs to MySQL, PostgreSQL and SQLite used for ChEMBL dumps.


```
from db_migrator import DbMigrator

migrator = DbMigrator('oracle://user:pass@ora-vm-065.ebi.ac.uk:1531/Chempro', postgresql://user:pass@porta-chembl-2.windows.ebi.ac.uk:5432/chembl_24)
migrator.migrate()

```
