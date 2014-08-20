import argparse

import benchmarks as bm

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="""
        Run a benchmark configured in a JSON file against a list of
        chosen Python ORM libraries.
        """
    )
    parser.add_argument(
        "--config", "-c", default="config.json",
        metavar="JSON_CONFIG_FILE", type=argparse.FileType("r"),
        help="""
        A JSON configuration file that specifies the details of the
        benchmark data set. For example,
        {
            "number_of_records": 10000
        }
        specifies that there should be 10000 testing records CRUDed
        by the ORMs in the benchmark session.
        """
    )
    parser.add_argument(
        "--orms", "-o",
        metavar="ORM", type=str, nargs="+",
        default=["SQLAlchemy", "SQLObject", "Storm", "peewee", "PonyORM"],
        help="""
        ORMs to participate in the benchmark. The list of ORMs includes:
        SQLAlchemy, SQLObject, Storm, peewee and PonyORM.
        """
    )
    parser.add_argument(
        "--sqlite", "-l",
        nargs="?", type=str, default="sqlite:///",
        help="""
        A DB connection string of SQLite, i.e., sqlite:///.
        Defaults to sqlite:///
        """
    )
    args = parser.parse_args()
    bm.perform_benchmarks(args)
