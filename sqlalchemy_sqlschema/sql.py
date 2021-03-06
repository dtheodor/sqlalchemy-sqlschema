# -*- coding: utf-8 -*-
"""
Contains the SQL expressions to get and set the SQL schema.

SQL expressions and their complilation were built using the following as a
guideline:
http://docs.sqlalchemy.org/en/latest/core/compiler.html#compiling-sub-elements-of-a-custom-expression-construct
"""

from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.ext.compiler import compiles

__all__ = ["get_schema", "set_schema"]


class GetSchema(Executable, ClauseElement):
    """Clause used to retrieve the active schema."""
    pass


class SetSchema(Executable, ClauseElement):
    """Clause used to set the active schema."""
    def __init__(self, schema):
        self.schema = schema


@compiles(GetSchema)
def _get_schema(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    # made up statement
    return "SHOW SCHEMA"


@compiles(SetSchema)
def _set_schema(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    # ANSI SQL statement
    return "SET SCHEMA {0}".format(element.schema)


@compiles(GetSchema, 'postgresql')
def _pg_show_search_path(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    return "SHOW search_path"


@compiles(SetSchema, 'postgresql')
def _pg_set_search_path(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    return "SET search_path TO {0}".format(element.schema)


@compiles(GetSchema, 'oracle')
def _oracle_current_schema(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    return "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual"


@compiles(SetSchema, 'oracle')
def _oracle_set_current_schema(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    return "ALTER SESSION SET CURRENT_SCHEMA = {0}".format(element.schema)


@compiles(GetSchema, 'mssql')
def _mssql_current_schema(element, compiler, **kw):
    # pylint: disable=unused-argument, missing-docstring
    return "SELECT SCHEMA_NAME()"


def get_schema():
    """An executable SQL Alchemy clause that can be used to get the active SQL
    schema.

    See also :func:`set_schema`.

    :Example:

    >>> stmt = get_schema()
    >>> stmt
    'SHOW search_path'
    >>> session.execute(stmt).scalar()
    'public'
    """
    return GetSchema()


def set_schema(schema):
    """An executeble SQL Alchemy clause that can be sed to set the active SQL
    schema.

    See also :func:`get_schema`.

    :Example:

    >>> stmt = set_schema('new_schema')
    >>> stmt
    'SET search_path TO new_schema'
    >>> session.execute(stmt)
    >>> assert session.execute(get_schema()).scalar() == 'new_schema'

    :param schema: :class:`str` to be set as the new SQL schema
    """
    return SetSchema(schema)
