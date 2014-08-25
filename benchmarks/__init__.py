import json

from .exc import ConfigurationError
from .sqlobject_benchmark import perform_sqlobject_benchmark


def perform_benchmarks(args):
    """
    Run benchmarks over all the ORMs indicated in args.orms.

    :param args: a parsed ArgumentParser() dictionary
    :return: True if benchmarks are performed correctly,
        False otherwise.
    """
    number_of_records = None
    number_of_repeats = None
    config = json.load(args.config)
    if 'number_of_records' in config:
        number_of_records = config['number_of_records']
    if hasattr(args, 'num_records'):
        number_of_records = args.num_records
    if 'number_of_repeats' in config:
        number_of_repeats = config['number_of_repeats']
    if hasattr(args, 'num_repeats'):
        number_of_repeats = args.num_repeats
    assert number_of_records is not None, \
        'missing parameter number_or_records'
    assert number_of_repeats is not None, \
        'missing parameter number_of_repeats'
    print(
        "Benchmark {0} on {1} records with "
        "each test aspect repeated {2} times".format(
            args.orms, number_of_records, number_of_repeats
        )
    )
    benchmark_result = dict()
    for key in 'sqlite', 'mysql', 'postgresql':
        conn_str = None
        if hasattr(args, key):
            conn_str = getattr(args, key)
        elif key in config:
            conn_str = config[key]
        if conn_str:
            perform_sqlobject_benchmark(
                key, conn_str, args, benchmark_result
            )
    return benchmark_result

def _get_connection_config(key, config, args):
    """
    Get the connection string for a database.

    :param key: a database such as 'sqlite', 'mysql' or 'postgresql'
    :param config: a dictionary which may include configuration for
        databases' connection strings specified in a JSON file
    :param args: a dictionary which may include configuration for
        databases' connection strings specified on the command line
    :return: a connection string for the database specified in `key`
        or raise a ConfigurationError if a connection string cannot
        be obtained from neither `config` nor `args`.

    Notice that `args` takes precedence over `config`.
    """
    conn_str = None
    if key in config:
        conn_str = config
    if hasattr(args, key):
        conn_str = getattr(args, key)
    if conn_str is None:
        raise ConfigurationError(
            "{0} is not configured in {1} nor {2}".format(
                key, config, args
            )
        )
    return conn_str
