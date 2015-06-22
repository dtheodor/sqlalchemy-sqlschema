# -*- coding: utf-8 -*-

from sqlalchemy_sqlschema.util import Stack

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