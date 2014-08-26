import __builtin__
import timeit

from sqlalchemy import Column, String, Integer, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    address = Column(String)
    person_id = Column(Integer, ForeignKey(Person.id))
    person = relationship(Person)


def _sqlalchemy_insert_data(test_data, session):
    for data in test_data:
        p = Person(name=data['person_name'])
        a = Address(address=data['address'], person=p)
        session.add(p)
        session.add(a)
    session.commit()


def _sqlalchemy_read_data(test_data, session):
    for data in test_data:
        p = session.query(Person).filter(Person.name == data['person_name']).one()
        a = session.query(Address).filter(Address.address == data['address']).one()


def _sqlalchemy_update_data(test_data, session):
    for data in test_data:
        p = session.query(Person).filter(Person.name == data['person_name']).one()
        p.name += '_suffix'
    session.commit()


def _sqlalchemy_delete_data(test_data, session):
    for data in test_data:
        session.query(Person).filter(
            Person.name == data['person_name'] + '_suffix'
        ).delete()
    session.commit()


__builtin__.__dict__.update(locals())


def perform_sqlalchemy_benchmark(database, conn_str, args, benchmark_result):
    if database == 'sqlite':
        if conn_str == ':memory:':
            conn_str = 'sqlite:///'
    engine = create_engine(conn_str)
    DBSession = sessionmaker()
    DBSession.configure(bind=engine)
    Base.metadata.create_all(engine)
    session = DBSession()
    __builtin__.__dict__.update(locals())
    if 'SQLAlchemy' not in benchmark_result:
        benchmark_result['SQLAlchemy'] = dict()
    if database not in benchmark_result['SQLAlchemy']:
        benchmark_result['SQLAlchemy'][database] = dict()
    test_aspects = ['insert', 'read', 'update', 'delete']
    timeit_funcs = [
        '_{0}_{1}_data(test_data, session)'.format(
            'sqlalchemy', test_aspect
        )
        for test_aspect in test_aspects
    ]
    for index, tf in enumerate(timeit_funcs):
        rst = timeit.timeit(tf, number=args.num_repeats)
        benchmark_result['SQLAlchemy'][database][test_aspects[index]] = rst
