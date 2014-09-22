import __builtin__
import timeit

from pony.orm import Database, Required, Set, select, db_session

from .lib import test_data_from_args, get_metadata_from_conn_str


@db_session
def _ponyorm_insert_data(test_data, person_cls, address_cls, db):
    for data in test_data:
        p = person_cls(name=data['person_name'])
        a = address_cls(address=data['address'], person=p)
    db.commit()


@db_session
def _ponyorm_read_data(test_data, person_cls, address_cls, db):
    for data in test_data:
        p = select(p for p in person_cls if person_cls.name == data['person_name'])
        a = select(a for a in address_cls if address_cls.address == data['address'])


@db_session
def _ponyorm_update_data(test_data, person_cls, address_cls, db):
    for data in test_data:
        p = person_cls.get(name=data['person_name'])
        p.name += '_suffix'
    db.commit()


@db_session
def _ponyorm_delete_data(test_data, person_cls, address_cls, db):
    for data in test_data:
        p = person_cls.get(name=data['person_name'] + '_suffix')
        p.delete()
    db.commit()


__builtin__.__dict__.update(locals())


def perform_ponyorm_benchmark(database, conn_str, args, benchmark_result):
    host, user, password, db = get_metadata_from_conn_str(conn_str)
    db = Database(database, host=host, user=user, passwd=password, db=db)

    class Person(db.Entity):
        name = Required(unicode)
        addresses = Set("Address")

    class Address(db.Entity):
        address = Required(unicode)
        person = Required(Person)

    db.generate_mapping(create_tables=True)

    test_data = test_data_from_args(args)
    assert test_data

    if 'ponyorm' not in benchmark_result:
        benchmark_result['ponyorm'] = dict()
    if database not in benchmark_result['ponyorm']:
        benchmark_result['ponyorm'][database] = dict()
    test_aspects = ['insert', 'read', 'update', 'delete']
    __builtin__.__dict__.update(locals())
    timeit_funcs = [
        '_{0}_{1}_data(test_data, Person, Address, db)'.format(
            'ponyorm', test_aspect
        )
        for test_aspect in test_aspects
    ]
    for index, tf in enumerate(timeit_funcs):
        rst = timeit.timeit(tf, number=args.num_repeats)
        benchmark_result['ponyorm'][database][test_aspects[index]] = rst
