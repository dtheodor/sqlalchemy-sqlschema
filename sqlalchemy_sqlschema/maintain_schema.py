# -*- coding: utf-8 -*-
"""
Provides the :func:`maintain_schema` context manager.
"""
from functools import wraps
from collections import defaultdict

from sqlalchemy import event
from sqlalchemy.orm import scoped_session

try:
    from gevent.local import local
except ImportError:
    from threading import local

from .sql import set_schema, get_schema
from .util import Stack

__all__ = ["maintain_schema"]


class SchemaContextManager(object):
    """Implements the context manager for applying the SQL schema, see
    :func:`maintain_schema`.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, schema, session):
        self.schema = schema
        self.session = session
        self.new_tx_listener = self._create_new_tx_listener(schema)
        # stores the schema to be restored on context manager exit
        self.prev_schema = None
        # stores the listener to be reinstated on context manager exit
        self.prev_listener = None

    _local = local()

    @classmethod
    def _get_schema_stack(cls, session):
        """Return a thread-local :class:`Stack` containing the schemas of the
        ``session``.

        We need to use a thread-local variable to store the previous active
        schema if we want to support nesting the context manager."""
        if not hasattr(cls._local, "schema_stacks"):
            cls._local.schema_stacks = defaultdict(Stack)
        # make sure to get the session object, not the scoped_session proxy
        session = session() if isinstance(session, scoped_session) else session
        # map a stack to the session
        return cls._local.schema_stacks[session]

    @staticmethod
    def _create_new_tx_listener(schema):
        """Create and return a function to be used with the "after_begin" SQL
        Alchemy event that will set the schema to ``schema``.
        """
        def set_schema_listener(session, transaction, connection):
            # pylint: disable=unused-argument, missing-docstring
            session.execute(set_schema(schema))
        return set_schema_listener

    @staticmethod
    def _cancel_listener(new_tx_listener, session):
        # pylint: disable=missing-docstring
        event.remove(session, "after_begin", new_tx_listener)

    @staticmethod
    def _enable_listener(new_tx_listener, session):
        # pylint: disable=missing-docstring
        event.listen(session, "after_begin", new_tx_listener)

    def __enter__(self):
        schema_stack = self._get_schema_stack(self.session)
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
        schema_stack = self._get_schema_stack(self.session)
        # 1. remove schema from the stack
        schema_stack.pop()
        # 2. stop the current listener
        self._cancel_listener(self.new_tx_listener, self.session)
        # 3. set the previous schema
        self.session.execute(set_schema(self.prev_schema))
        # 4. bring back the previous listener
        if self.prev_listener:
            self._enable_listener(self.prev_listener, self.session)

    def __call__(self, f):
        # pylint: disable=invalid-name, missing-docstring
        @wraps(f)
        def decorated(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return decorated


def maintain_schema(schema, session):
    """Context manager/decorator that will apply the SQL schema ``schema`` using
    the ``session``. The ``schema`` will persist across different transactions,
    if these happen within the context manager's body.

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
