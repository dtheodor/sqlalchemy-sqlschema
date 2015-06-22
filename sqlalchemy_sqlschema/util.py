# -*- coding: utf-8 -*-

class Stack(list):
    """A :class:`list` with `top`, `pop`, and `push` methods to give it a
    stack-like API.
    """
    __slots__ = ()

    @property
    def top(self):
        """Return the last element or None if the list is empty."""
        return self[-1] if self else None

    push = list.append

    def pop(self):
        """:meth:`list.pop` that returns `None` if the list is empty"""
        try:
            return list.pop(self)
        except IndexError:
            return None