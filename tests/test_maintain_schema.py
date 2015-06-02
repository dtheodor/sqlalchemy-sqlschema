# -*- coding: utf-8 -*-
"""
Test maintain_schema contextmanager
"""
import mock
import pytest

from sqlalchemy_sqlschema import maintain_schema
from sqlalchemy_sqlschema.maintain_schema import Stack

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


class TestStack(object):

    def test_init(self):
        s = Stack([1,2,3,4])
        assert s == [1,2,3,4]
        assert len(s) == 4

    def test_push(self):
        s = Stack([1,2,3,4])
        s.push(5)
        assert s[-1] == 5

    def test_pop(self):
        s = Stack([1,2])
        assert s.pop() == 2
        assert s.pop() == 1
        assert s.pop() is None
        assert s.pop() is None
        assert len(s) == 0

    def test_top(self):
        s = Stack([1,2])
        assert s.top == 2
        s = Stack()
        assert s.top is None

