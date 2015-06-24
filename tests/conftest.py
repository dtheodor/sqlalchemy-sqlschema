# -*- coding: utf-8 -*-
"""
Py.test fixtures to setup database engines and the test schema.
"""
from __future__ import print_function

import os
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser

import pytest
from sqlalchemy import create_engine, pool, event
from sqlalchemy.orm import sessionmaker

CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.environ.get("TEST_CONFIG", 'test.config'))

DEFAULT_TEST_SCHEMA = "test_schema"

class DbConfig(object):

    @classmethod
    def from_env(cls):
        pass

    @classmethod
    def from_configparser(cls, conf):
        pass

class PgConfig(DbConfig):
    key = "postgresql"

    def __init__(self, db_url, echo, test_schema):
        self.db_url = db_url
        self.echo = echo
        self.test_schema = test_schema

    @classmethod
    def from_env(cls):
        """Create a ``PgConfig`` from the following environment variables:
        POSTGRESQL_URL
        POSTGRESQL_ECHO
        POSTGRESQL_SCHEMA
        """
        prefix = cls.key.upper() + "_"
        if prefix + "URL" in os.environ:
            pg_config = cls(
                db_url=os.environ.get(prefix + "URL"),
                echo=prefix + "ECHO" in os.environ,
                test_schema=os.environ.get(prefix + "SCHEMA",
                                           DEFAULT_TEST_SCHEMA)
            )
            return pg_config

    @classmethod
    def from_configparser(cls, conf):
        if conf.has_section(cls.key):
            pg_config = cls(
                db_url=conf.get(cls.key, "url"),
                echo=conf.getboolean(cls.key, "echo"),
                test_schema=conf.get(cls.key, "schema")
            )
            return pg_config

class OracleConfig(DbConfig):
    key = "oracle"

    def __init__(self, db_url, echo, test_schema1, test_schema2, test_schema3):
        self.db_url = db_url
        self.echo = echo
        self.test_schema1 = test_schema1
        self.test_schema2 = test_schema2
        self.test_schema3 = test_schema3

    @classmethod
    def from_configparser(cls, conf):
        if conf.has_section(cls.key):
            oracle_config = cls(
                db_url=conf.get(cls.key, "url"),
                echo=conf.getboolean(cls.key, "echo"),
                test_schema1=(conf.get(cls.key, "schema1"),
                              conf.get(cls.key, "password1")),
                test_schema2=(conf.get(cls.key, "schema2"),
                              conf.get(cls.key, "password2")),
                test_schema3=(conf.get(cls.key, "schema3"),
                              conf.get(cls.key, "password3"))
            )
            return oracle_config

class MssqlConfig(DbConfig):
    key = "mssql"

db_config_classes = (PgConfig, MssqlConfig, OracleConfig)

@pytest.fixture(scope="session")
def test_dbs():
    """Return a dictionary mapping database keys to their corresponding
    configuration
    """
    db_configs = {cls.key: None for cls in db_config_classes}

    update_config_from_file(db_configs)
    update_config_from_env(db_configs)

    return db_configs

def update_config_from_file(db_configs):
    """Update each value of the `db_configs` if there is a corresponding entry
    in the configuration file.
    """
    conf = ConfigParser(defaults={"echo": "off",
                                  "schema": DEFAULT_TEST_SCHEMA})
    if not os.path.isfile(CONFIG_FILE):
        return

    conf.read(CONFIG_FILE)
    for cls in db_config_classes:
        db_config = cls.from_configparser(conf)
        if db_config:
            db_configs[cls.key] = db_config

def update_config_from_env(db_configs):
    """Update each value of the `db_configs` from environment variables
    """
    for cls in db_config_classes:
        db_config = cls.from_env()
        if db_config:
            db_configs[cls.key] = db_config


@pytest.fixture(scope="session")
def pg_engine(test_dbs):
    """Initialize and return an SQL alchemy postgresql engine to be used with
    tests. Runs once for the test session.
    """
    pg_config = test_dbs["postgresql"]
    if pg_config is None:
        pytest.skip("No PostgreSQL database configured.")

    print("SQL Alchemy tetscase: Using database '{}'".format(pg_config.db_url))
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
    set_schema_sql = "SET search_path TO {}".format(pg_test_schema)
    clear_event = execute_sql_on_connection(pg_engine, set_schema_sql)
    yield
    clear_event()

def execute_sql_on_connection(engine, sql):
    """Create an event that runs the passed ``sql`` statement every time
    a new connection is made to the db.

    :return: a function that will clear the event
    """
    @event.listens_for(engine, "connect")
    def init_search_path(connection, conn_record):
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
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

@pytest.fixture(scope="session")
def oracle_engine(test_dbs):
    """Initialize and return an SQL alchemy Oracle engine to be used with
    tests. Runs once for the test session.
    """
    oracle_config = test_dbs["oracle"]
    if oracle_config is None:
        pytest.skip("No Oracle database configured.")

    print("SQL Alchemy tetscase: Using database '{}'".format(oracle_config.db_url))
    engine = create_engine(oracle_config.db_url,
                           echo=oracle_config.echo,
                           poolclass=pool.NullPool)
    return engine

@pytest.yield_fixture(scope="session")
def oracle_session_factory(oracle_engine):
    sessionmaker_ = sessionmaker(bind=oracle_engine)
    yield sessionmaker_
    sessionmaker_.close_all()

@pytest.yield_fixture
def oracle_session(oracle_session_factory):
    session = oracle_session_factory()
    yield session
    session.close()

def create_oracle_schema(engine, schema, passwd):
    def drop_schema():
        try:
            engine.execute(
                "DROP USER {schema} CASCADE".format(schema=schema))
        except:
            pass
    drop_schema()
    engine.execute(
        "CREATE USER {schema} IDENTIFIED BY {passwd}".format(
            schema=schema, passwd=passwd))
    engine.execute("GRANT dba TO {schema}".format(schema=schema))
    return drop_schema

@pytest.yield_fixture(scope="session")
def oracle_schema1(oracle_engine, test_dbs):
    """Create a schema within which the tests will be run, return the name of
    that schema, and drop it on cleanup
    """
    oracle_config = test_dbs["oracle"]
    test_schema, passwd = oracle_config.test_schema1

    drop_schema = create_oracle_schema(oracle_engine, test_schema, passwd)
    print("Created Oracle schema '{}'".format(test_schema))
    yield test_schema.upper()
    drop_schema()

@pytest.yield_fixture(scope="session")
def oracle_schema2(oracle_engine, test_dbs):
    oracle_config = test_dbs["oracle"]
    test_schema, passwd = oracle_config.test_schema2

    drop_schema = create_oracle_schema(oracle_engine, test_schema, passwd)
    print("Created Oracle schema '{}'".format(test_schema))
    yield test_schema.upper()
    drop_schema()

@pytest.yield_fixture(scope="session")
def oracle_schema3(oracle_engine, test_dbs):
    oracle_config = test_dbs["oracle"]
    test_schema, passwd = oracle_config.test_schema3

    drop_schema = create_oracle_schema(oracle_engine, test_schema, passwd)
    print("Created Oracle schema '{}'".format(test_schema))
    yield test_schema.upper()
    drop_schema()


@pytest.yield_fixture
def maintain_oracle_test_schema(oracle_engine, oracle_schema1):
    """Set the oracle schema to `pg_test_schema` on all new db connections
    """
    set_schema_sql = "ALTER SESSION SET CURRENT_SCHEMA = {}".format(oracle_schema1)
    clear_event = execute_sql_on_connection(oracle_engine, set_schema_sql)
    yield
    clear_event()
