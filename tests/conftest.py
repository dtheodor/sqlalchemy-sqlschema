# -*- coding: utf-8 -*-
"""
Py.test fixtures to setup database engines and the test schema.
"""
import os
from ConfigParser import SafeConfigParser
from collections import namedtuple

import pytest
from sqlalchemy import create_engine, pool, event
from sqlalchemy.orm import sessionmaker

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
def test_dbs():
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
def pg_engine(test_dbs):
    """Initialize and return an SQL alchemy postgresql engine to be used with
    tests. Runs once for the test session.
    """
    pg_config = test_dbs["postgresql"]
    if pg_config is None:
        pytest.skip("No PostgreSQL database configured.")

    print "SQL Alchemy tetscase: Using database '{}'".format(pg_config.db_url)
    engine = create_engine(pg_config.db_url,
                           echo=pg_config.echo,
                           poolclass=pool.NullPool)
    return engine

@pytest.yield_fixture(scope="session")
def pg_session_factory(pg_engine):
    sessionmaker_ = sessionmaker(bind=pg_engine)
    yield sessionmaker_
    sessionmaker_.close_all()

@pytest.yield_fixture
def pg_session(pg_session_factory):
    session = pg_session_factory()
    yield session
    session.close()

@pytest.yield_fixture(scope="session")
def pg_test_schema(pg_engine, test_dbs):
    """Create a schema within which the tests will be run, return the name of
    that schema, and drop it on cleanup
    """
    pg_config = test_dbs["postgresql"]
    test_schema = pg_config.test_schema

    def drop_schema():
        pg_engine.execute(
            "DROP SCHEMA IF EXISTS {schema} CASCADE".format(schema=test_schema))
    drop_schema()
    pg_engine.execute(
        "CREATE SCHEMA {schema}".format(schema=test_schema))
    print("Created Postgres schema '{}'".format(test_schema))
    yield test_schema
    drop_schema()


@pytest.yield_fixture
def maintain_pg_test_schema(pg_engine, pg_test_schema):
    """Set the postgres schema to `pg_test_schema` on all new db connections
    """
    clear_event = maintain_pg_schema(pg_engine, pg_test_schema)
    yield
    clear_event()

def maintain_pg_schema(engine, schema):
    """Create an event that sets the search_path to "`schema`,public" every time
    a new connection is made to the db.

    :return: a function that will clear the event
    """
    set_schema_sql = "SET search_path TO {}".format(schema)

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

@pytest.fixture(scope="session")
def mssql_engine(test_dbs):
    """Initialize and return an SQL alchemy MS SQL engine to be used with
    tests. Runs once for the test session.
    """
    mssql_config = test_dbs["mssql"]
    if mssql_config is None:
        pytest.skip("No MS SQL database configured.")

