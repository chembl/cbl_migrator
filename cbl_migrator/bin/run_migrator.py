from cbl_migrator import DbMigrator
import argparse
import sys


def run(origin, dest, n_workers, copy_schema, copy_data, copy_constraints, copy_indexes, chunk_size):
    migrator = DbMigrator(origin, dest, n_workers=int(n_workers))
    migrator.migrate(copy_schema=copy_schema, copy_data=copy_data,
                     copy_constraints=copy_constraints, copy_indexes=copy_indexes, chunk_size=int(chunk_size))


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Migrate an Oracle database to a PosgreSQL, MySQL or SQLite server')
    parser.add_argument('origin',
                        help='Origin database connection string',
                        default=None)
    parser.add_argument('dest',
                        help='Destination database connection string',
                        default=None)

    parser.add_argument('--n_workers',
                        help='Number of workers migrating tables in parallel',
                        default=8)

    parser.add_argument('--copy_schema',
                        help='Copy database schema',
                        default=True)

    parser.add_argument('--copy_data',
                        help='Copy data',
                        default=True)

    parser.add_argument('--copy_constraints',
                        help='Copy constraints',
                        default=True)

    parser.add_argument('--copy_indexes',
                        help='Copy indexes',
                        default=True)

    parser.add_argument('--chunk_size',
                        help='Number of rows copied at the same time',
                        default=1000)

    args = parser.parse_args()
    run(args.origin, args.dest, args.n_workers, args.copy_schema,
        args.copy_data, args.copy_constraints, args.copy_indexes, args.chunk_size)


if __name__ == '__main__':
    main()
