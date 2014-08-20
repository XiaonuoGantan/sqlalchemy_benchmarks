import json


def perform_benchmarks(args):
    """
    Run benchmarks over all the ORMs indicated in args.orms.

    :param args: a parsed ArgumentParser() dictionary
    :return: True if benchmarks are performed correctly,
        False otherwise.
    """
    config = json.load(args.config)
    number_of_records = config['number_of_records']
    print("Benchmark {0} on {1} records".format(
        args.orms, number_of_records
    ))
