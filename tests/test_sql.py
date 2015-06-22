# -*- coding: utf-8 -*-
"""
Created by @dtheodor at 2015-05-31

Test SQL compilation of SQL schema set and get.
"""
from sqlalchemy.dialects.postgres import dialect as pg_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy_sqlschema.sql import set_schema, get_schema

class TestDefaultSqlCompilation(object):
    def test_default_get_schema(self):
        assert str(get_schema()) == "SHOW SCHEMA"

    def test_default_set_schema(self):
        assert str(set_schema("new_schema")) == "SET SCHEMA new_schema"

class TestPostgresSqlCompilation(object):
    def test_pg_get_schema(self):
        get_schema_stmt = get_schema()
        assert str(get_schema_stmt.compile(dialect=pg_dialect())) == \
               "SHOW search_path"

    def test_pg_set_schema(self):
        set_schema_stmt = set_schema("new_schema")
        assert str(set_schema_stmt.compile(dialect=pg_dialect())) == \
               "SET search_path TO new_schema"

class TestOracleCompilation(object):
    def test_pg_get_schema(self):
        get_schema_stmt = get_schema()
        assert str(get_schema_stmt.compile(dialect=oracle_dialect())) == \
               "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual"

    def test_pg_set_schema(self):
        set_schema_stmt = set_schema("new_schema")
        assert str(set_schema_stmt.compile(dialect=oracle_dialect())) == \
               "ALTER SESSION SET CURRENT_SCHEMA = new_schema"