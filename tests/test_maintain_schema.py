# -*- coding: utf-8 -*-
"""
Test maintain_schema contextmanager
"""
import mock
import pytest

from sqlalchemy_sqlschema import maintain_schema

def test_decorator():
    """Test that using as a decorator triggers the context manager"""
    from sqlalchemy_sqlschema.maintain_schema import SchemaContextManager
    with mock.patch.object(
            SchemaContextManager, "__enter__", mock.Mock(return_value='foo')), \
         mock.patch.object(
             SchemaContextManager, "__exit__", mock.Mock(return_value=False)):

        m = maintain_schema("schema", None)

        @m
        def a_function():
            pass

        a_function()

        assert m.__enter__.call_count == 1
        assert m.__exit__.call_count == 1


