# -*- coding: utf-8 -*-
"""
Provides the :func:`maintain_schema` context manager.
"""
from sqlalchemy import event

try:
    from gevent.local import local
except ImportError:
    from threading import local

from .sql import set_schema, get_schema

__all__ = ["maintain_schema"]

class _Stack(list):
    """A :class:`list` with `top`, `pop`, and `push` methods to give it a
    stack-like API.
    """
    __slots__ = ()

    @property
    def top(self):
        return self[-1] if self else None

    push = list.append

    def pop(self):
        try:
            list.pop(self)
        except IndexError:
            return None


class SchemaContextManager(object):
    """Implements the context manager for applying the SQL schema, see
    :func:`maintain_schema`.
    """
    def __init__(self, schema, session):
        self.schema = schema
        self.session = session
        self.new_tx_listener = self._create_new_tx_listener(schema)

    _local = local()

    @classmethod
    def _get_schema_stack(cls):
        if not hasattr(cls._local, "stack"):
            cls._local.stack = _Stack()
        return cls._local.stack

    def _create_new_tx_listener(self, schema):
        def set_schema_listener(session, transaction, connection):
            session.execute(set_schema(schema))

        return set_schema_listener

    @staticmethod
    def _cancel_listener(new_tx_listener, session):
        event.remove(session, "after_begin", new_tx_listener)

    @staticmethod
    def _enable_listener(new_tx_listener, session):
        event.listen(session, "after_begin", new_tx_listener)

    def __enter__(self):
        schema_stack = self._get_schema_stack()
        if schema_stack.top is None:
            schema_stack.push(
                (self.session.execute(get_schema()).scalar(), None))
        # 1. get the prev_schema
        self.prev_schema, self.prev_listener = schema_stack.top
        # 2. supress previous listeners (will be reinstated in the __exit__)
        if self.prev_listener:
            self._cancel_listener(self.prev_listener, self.session)

        # 3. set the new schema
        self.session.execute(set_schema(self.schema))
        # 4. set a new listener for it
        self._enable_listener(self.new_tx_listener, self.session)
        # 5. push it to the stack
        schema_stack.push((self.schema, self.new_tx_listener))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        schema_stack = self._get_schema_stack()
        # 1. remove schema from the stack
        schema_stack.pop()
        # 2. stop the current listener
        self._cancel_listener(self.new_tx_listener, self.session)
        # 3. set the previous schema
        self.session.execute(set_schema(self.prev_schema))
        # 4. bring back the previous listener
        if self.prev_listener:
            self._enable_listener(self.prev_listener, self.session)


def maintain_schema(schema, session):
    """Context manager that will apply the SQL schema ``schema`` using the
    ``session``. The ``schema`` will persist across different transactions, if
    these happen within the context manager's body.

    After the context manager exits, it will restore the SQL schema that was
    found to be active when it was entered.

    The context manager can also be nested. Exiting the nested context manager
    will restore the SQL schema set by the outer context manager.

    :Example:

    >>> assert session.execute('SHOW search_path').scalar() == 'public'
    >>> with maintain_schema('new_schema', session):
    >>>     assert session.execute('SHOW search_path').scalar() == 'new_schema'
    >>>     # still maintained after a rollback
    >>>     session.rollback()
    >>>     assert session.execute('SHOW search_path').scalar() == 'new_schema'
    >>> # back to original
    >>> assert session.execute('SHOW search_path').scalar() == 'public'

    :param schema: :class:`str` to be set as the SQL schema
    :param session: a :class:`~sqlalchemy.orm.session.Session` which will be
        used to set the SQL schema
    """
    return SchemaContextManager(schema, session)
