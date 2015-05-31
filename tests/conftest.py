# -*- coding: utf-8 -*-
"""
Created on May 08, 2015

@author: dimitris.theodorou

Py.test fixtures.
"""
import os
from ConfigParser import SafeConfigParser
from collections import namedtuple

import pytest
from sqlalchemy import create_engine, pool, event
from sqlalchemy.orm import sessionmaker, scoped_session

CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.environ.get("TEST_CONFIG", 'test.config'))

DEFAULT_TEST_SCHEMA = "test_schema"

DbConfig = namedtuple("DbConfig", [
    "db_url",
    "echo",
    "test_schema"
])

@pytest.fixture(scope="session")
def test_db_configurations():
    """Return a dictionary mapping database keys to their corresponding sql
    alchemy configuration
    """
    conf = SafeConfigParser(
        defaults={"echo": "off",
                  "schema": DEFAULT_TEST_SCHEMA})
    if not os.path.isfile(CONFIG_FILE):
        raise Exception("Expected to find a '{}' file but no "
                        "such file was found.".format(CONFIG_FILE))
    conf.read(CONFIG_FILE)
    db_configs = {"postgresql": None,
                  "mysql": None,
                  "oracle": None}
    for db_section in db_configs.keys():
        if conf.has_section(db_section):
            db_configs[db_section] = DbConfig(
                db_url=conf.get(db_section, "sqlalchemy.url"),
                echo=conf.getboolean(db_section, "echo"),
                test_schema=conf.get(db_section, "schema")
            )
    return db_configs


@pytest.fixture(scope="session")
def postgresql_engine(test_db_configurations):
    """Initialize and return an SQL alchemy postgresql engine to be used with
    tests. Runs once for the test session.
    """
    pg_config = test_db_configurations["postgresql"]
    if pg_config is None:
        return None

    print "SQL Alchemy tetscase: Using database '{}'".format(pg_config.db_url)
    engine = create_engine(pg_config.db_url,
                           echo=pg_config.echo,
                           poolclass=pool.NullPool)

    engine.execute("CREATE EXTENSION IF NOT EXISTS hstore")
    engine.execute("CREATE EXTENSION IF NOT EXISTS citext")
    engine.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    return engine

@pytest.yield_fixture(scope="session")
def session_factory(postgresql_engine):
    sessionmaker_ = sessionmaker(bind=postgresql_engine)
    yield sessionmaker_
    sessionmaker_.close_all()

@pytest.yield_fixture
def session(session_factory):
    session = session_factory()
    yield session
    session.close()

@pytest.fixture
def reset_schema_for_session(session):
    session.execute("set search_path to public;")

@pytest.fixture(scope="session")
def test_schema(request, sqlalchemy_engine, test_config):
    """Create a schema within which the tests will be run, return the name of
    that schema, and drop it to cleanup
    """
    test_schema_name = test_config.test_schema
    return _new_schema(test_schema_name, request, sqlalchemy_engine)


def _new_schema(schema_name, request, engine):
    """Create a new schema with name `schema_name`, return that name, and drop
    it on cleanup
    """
    def drop_schema():
        engine.execute("DROP SCHEMA IF EXISTS {schema} CASCADE".format(
            schema=schema_name))

    drop_schema()
    engine.execute("CREATE SCHEMA {schema}".format(schema=schema_name))
    print("Created schema '{}'".format(schema_name))
    request.addfinalizer(drop_schema)
    return schema_name

@pytest.yield_fixture
def maintain_test_schema(sqlalchemy_engine, test_schema):
    """Set the postgres schema to `test_schema` on all new db connections
    """
    clear_event = maintain_pg_schema(sqlalchemy_engine, test_schema)
    yield
    clear_event()

def maintain_pg_schema(engine, schema):
    """Create an event that sets the search_path to "`schema`,public" every time
    a new connection is made to the db.

    :return: function: a function that will clear the event
    """
    search_path = schema + ",public"  # need public as well for extensions
    set_schema_sql = "SET search_path TO {}".format(search_path)

    @event.listens_for(engine, "connect")
    def init_search_path(connection, conn_record):
        cursor = connection.cursor()
        try:
            cursor.execute(set_schema_sql)
        finally:
            cursor.close()

    def clear_event():
        event.remove(engine, "connect", init_search_path)

    return clear_event


@pytest.yield_fixture
def lightweight_sqlalchemy_session(request, sqlalchemy_engine):
    """Create a "lightweight" sql alchemy scoped_session to be used in tests.

    The session commits and rollbacks all changes within SAVEPOINTS that exist
    within a single transaction, so that everything can be undone at the end
    of the test by doing a transaction.rollback(). This is faster than the
    alternative of doing full-blown transactions and truncating all tables
    as cleanup
    """
    connection = sqlalchemy_engine.connect()

    # begin a non-ORM transaction
    trans = connection.begin()

    # bind an individual Session to the connection
    session = scoped_session(sessionmaker(bind=connection))

    # start the session in a SAVEPOINT...
    session.begin_nested()

    # then each time that SAVEPOINT ends, reopen it
    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(session, transaction):
        if transaction.nested and not transaction._parent.nested:
            session.expire_all()
            # expire all session-held objects to have the same behavior
            # of a regular commit() or rollback()
            session.begin_nested()

    if request.instance:
        request.instance.connection = connection
        request.instance.trans = trans
        request.instance.session = session

    yield session

    session.rollback()
    session.remove()

    # rollback - everything that happened with the
    # Session above (including calls to commit())
    # is rolled back.
    trans.rollback()

    # return connection to the Engine
    connection.close()
