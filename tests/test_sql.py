"""
Created by @dtheodor at 2015-05-31

Test SQL compilation of SQL schema set and get.
"""
import pytest
from sqlalchemy.dialects.postgres import dialect as pg_dialect
from sqlalchemy_sqlschema.sql import set_schema, get_schema

class TestDefaultSqlCompilation(object):
    def test_default_get_schema(self):
        with pytest.raises(NotImplementedError):
            str(get_schema())

    def test_default_set_schema(self):
        with pytest.raises(NotImplementedError):
            str(set_schema("new_schema"))

class TestPostgresSqlCompilation(object):
    def test_pg_get_schema(self):
        get_schema_stmt = get_schema()
        assert str(get_schema_stmt.compile(dialect=pg_dialect())) == \
               "SHOW search_path"

    def test_pg_set_schema(self):
        set_schema_stmt = set_schema("new_schema")
        assert str(set_schema_stmt.compile(dialect=pg_dialect())) == \
               "SET search_path TO new_schema"
