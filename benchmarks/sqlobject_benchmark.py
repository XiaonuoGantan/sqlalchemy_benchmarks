import __builtin__
import timeit

from sqlobject import StringCol, SQLObject, ForeignKey, sqlhub, connectionForURI

from .lib import record_benchmark_result


class Person(SQLObject):
    name = StringCol()


class Address(SQLObject):
    address = StringCol()
    person = ForeignKey('Person')


def _sqlobject_insert_data(test_data):
    for data in test_data:
        p = Person(name=data['person_name'])
        a = Address(address=data['address'], person=p)


def _sqlobject_read_data(test_data):
    for data in test_data:
        p = Person.select(Person.q.name == data['person_name'])[0]
        a = Address.select(Address.q.person == p)[0]


def _sqlobject_update_data(test_data):
    for data in test_data:
        person = Person.select(Person.q.name == data['person_name'])[0]
        person.name += "_suffix"


def _sqlobject_delete_data(test_data):
    for data in test_data:
        person = Person.select(
            Person.q.name == (data['person_name'] + '_suffix')
        )[0]
        Person.delete(person.id)

test_data = []
__builtin__.__dict__.update(locals())


def perform_sqlobject_benchmark(database, conn_str, args, benchmark_result):
    sqlhub.processConnection = connectionForURI(conn_str)
    Person.createTable()
    Address.createTable()
    for x in range(args.num_records):
        data = {
            'person_name': 'Person_{0}'.format(x),
            'address': 'Address_{0}'.format(x)
        }
        test_data.append(data)
    record_benchmark_result(
        benchmark_result, 'SQLObject', database, args.num_repeats
    )
