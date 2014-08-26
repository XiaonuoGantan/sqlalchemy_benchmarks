import argparse
import os

from pprint import pprint

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
            "number_of_records": 10000,
            "sqlite": "sqlite:///"
        }
        specifies that there should be 10000 testing records CRUDed
        by the ORMs in the benchmark session and the SQLite connection
        should be made against a in-memory SQLite database.
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
        nargs="?", type=str,
        help="""
        A DB connection string of SQLite, i.e., sqlite:///.
        Defaults to sqlite:///
        """
    )
    parser.add_argument(
        "--num-records", "-n",
        nargs="?", type=int,
        help="""
        Specify how many random records you'd like to generate as the
        test data.
        """
    )
    args = parser.parse_args()
    rst = bm.perform_benchmarks(args)
    sqlite_file_path = os.path.join(os.path.dirname(__file__), 'sqlite')
    if os.path.exists(sqlite_file_path):
        os.remove(sqlite_file_path)
    print("Benchmark results:")
    pprint(rst)
