import __builtin__
import timeit

from storm.locals import Int, Reference, RawStr, create_database, Store
from storm.exceptions import NotOneError

from .lib import record_benchmark_result, test_data_from_args


class Person(object):
    __storm_table__ = 'person'
    id = Int(primary=True)
    name = RawStr()


class Address(object):
    __storm_table__ = 'address'
    id = Int(primary=True)
    address = RawStr()
    person_id = Int()
    person = Reference(person_id, Person.id)


def _storm_insert_data(test_data, store):
    for data in test_data:
        p = Person()
        p.name = data['person_name']
        a = Address()
        a.name = data['address']
        a.person = p
        store.add(p)
        store.add(a)
    store.flush()


def _storm_read_data(test_data, store):
    for data in test_data:
        person = store.find(
            Person, Person.name == data['person_name']
        ).one()
        address = store.find(
            Address, Address.person == person
        ).one()


def _storm_update_data(test_data, store):
    for data in test_data:
        person = store.find(
            Person, Person.name == data['person_name']
        ).set(name=data['person_name'] + '_suffix')
    store.flush()


def _storm_delete_data(test_data, store):
    for data in test_data:
        person = store.find(
            Person, Person.name == data['person_name'] + '_suffix'
        ).one()
        store.remove(person)
    store.flush()


test_data = []
__builtin__.__dict__.update(locals())


def perform_storm_benchmark(database, conn_str, args, benchmark_result):
    if database == 'sqlite':
        if conn_str == ':memory:':
            conn_str = 'sqlite:'
    db = create_database(conn_str)
    store = Store(db)
    store.execute("CREATE TABLE person "
                  "(id INTEGER PRIMARY KEY, name VARCHAR)")
    store.execute("CREATE TABLE address "
                  "(id INTEGER PRIMARY KEY, address VARCHAR, person_id INTEGER, "
                  " FOREIGN KEY(person_id) REFERENCES person(id))")
    __builtin__.__dict__.update(locals())
    test_data = test_data_from_args(args)
    if 'Storm' not in benchmark_result:
        benchmark_result['Storm'] = dict()
    if database not in benchmark_result['Storm']:
        benchmark_result['Storm'][database] = dict()
    test_aspects = ['insert', 'read', 'update', 'delete']
    timeit_funcs = [
        "_{0}_{1}_data(test_data, store)".format(
            'storm', test_aspect
        )
        for test_aspect in test_aspects
    ]
    for index, tf in enumerate(timeit_funcs):
        rst = timeit.timeit(tf, number=args.num_repeats)
        benchmark_result['Storm'][database][test_aspects[index]] = rst
