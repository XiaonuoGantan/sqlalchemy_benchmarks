import __builtin__
import timeit

from peewee import SqliteDatabase, MySQLDatabase, PostgresqlDatabase, \
    CharField, ForeignKeyField, Model

from .lib import test_data_from_args, record_benchmark_result


def _peewee_insert_data(test_data, person_cls, address_cls):
    for data in test_data:
        p = person_cls(name=data['person_name'])
        p.save()
        a = address_cls(address=data['address'], person=p)
        a.save()


def _peewee_read_data(test_data, person_cls, address_cls):
    for data in test_data:
        p = person_cls.select().where(person_cls.name == data['person_name']).get()
        a = address_cls.select().where(address_cls.address == data['address']).get()


def _peewee_update_data(test_data, person_cls, address_cls):
    for data in test_data:
        p = person_cls.update(name=data['person_name'] + '_suffix').where(
            person_cls.name == data['person_name']
        )

def _peewee_delete_data(test_data, person_cls, address_cls):
    for data in test_data:
        p = person_cls.select().where(person_cls.name == data['person_name']).get()
        p.delete()


__builtin__.__dict__.update(locals())


def perform_peewee_benchmark(database, conn_str, args, benchmark_result):
    if database == 'sqlite':
        db = SqliteDatabase(database)
    elif database == 'mysql':
        db = MySQLDatabase(database)
    elif database == 'postgresql':
        db = PostgresqlDatabase(database)

    class Person(Model):
        name = CharField()
        class Meta:
            database = db

    class Address(Model):
        address = CharField()
        person = ForeignKeyField(Person)
        class Meta:
            database = db

    Person.create_table()
    Address.create_table()

    test_data = test_data_from_args(args)
    assert test_data

    if 'peewee' not in benchmark_result:
        benchmark_result['peewee'] = dict()
    if database not in benchmark_result['peewee']:
        benchmark_result['peewee'][database] = dict()
    test_aspects = ['insert', 'read', 'update', 'delete']
    timeit_funcs = [
        '_{0}_{1}_data(test_data, Person, Address)'.format(
            'peewee', test_aspect
        )
        for test_aspect in test_aspects
    ]
    for index, tf in enumerate(timeit_funcs):
        rst = timeit.timeit(tf, number=args.num_repeats)
        benchmark_result['peewee'][database][test_aspects[index]] = rst
