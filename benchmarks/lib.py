import timeit


def record_benchmark_result(benchmark_result, orm, database, timeit_times):
    if orm not in benchmark_result:
        benchmark_result[orm] = dict()
    if database not in benchmark_result[orm]:
        benchmark_result[orm][database] = dict()
    orm_lowercase = orm.lower()
    test_aspects = ['insert', 'read', 'update', 'delete']
    timeit_funcs = [
        "_{0}_{1}_data(test_data)".format(
            orm_lowercase, test_aspect
        )
        for test_aspect in test_aspects
    ]
    for index, tf in enumerate(timeit_funcs):
        rst = timeit.timeit(tf, number=timeit_times)
        benchmark_result[orm][database][test_aspects[index]] = rst
