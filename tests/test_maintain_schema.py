# -*- coding: utf-8 -*-
"""
Test maintain_schema contextmanager
"""
try:
    from unittest import mock
except:
    import mock
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.orm import Session, scoped_session

from sqlalchemy_sqlschema import maintain_schema
from sqlalchemy_sqlschema.maintain_schema import SchemaContextManager
from sqlalchemy_sqlschema.sql import GetSchema, SetSchema

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


@pytest.fixture(scope="session")
def engine():
    return create_engine('sqlite://')

def _mock_session(engine):
    """Return an sqlite session where get and set schema execution is mocked
    out."""
    session = Session(engine)
    original_execute = session.execute
    class GetSchemaResult:
        def scalar(self):
            return "default_schema"

    def execute(stmt, *args, **kwargs):
        if isinstance(stmt, GetSchema):
            return GetSchemaResult()
        elif isinstance(stmt, SetSchema):
            return original_execute("select 1")
        else:
            return original_execute(stmt, *args, **kwargs)

    patcher = mock.patch.object(session, "execute", autospec=True, side_effect=execute)
    return session, patcher

@pytest.yield_fixture
def mock_session(engine):
    session, patcher = _mock_session(engine)
    patcher.start()
    yield session
    patcher.stop()
    session.close()

@pytest.yield_fixture
def mock_session2(engine):
    session, patcher = _mock_session(engine)
    patcher.start()
    yield session
    patcher.stop()
    session.close()

@pytest.yield_fixture
def Model(engine):
    from sqlalchemy import Column, Integer
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Model(Base):
        __tablename__ = "model"
        id = Column(Integer, primary_key=True)

    Base.metadata.create_all(engine)
    yield Model
    Base.metadata.drop_all(engine)


class TestSessionMaintainSchema(object):

    @pytest.yield_fixture(autouse=True)
    def set_session_and_previous_schema(self, mock_session):
        """Push a schema in the schema stack so the stack always starts with a
        previous schema to revert to, and set self.session attribute"""
        SchemaContextManager._get_schema_stack(mock_session).push(("schema1", None))
        self.session = mock_session
        yield
        SchemaContextManager._get_schema_stack(mock_session).pop()

    def test_maintain_schema(self):
        m = maintain_schema("schema2", self.session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert self.session.execute.call_count == 1
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema1"
            assert self.session.execute.call_count == 2
            assert m.new_tx_listener.called == False

    def test_maintain_schema_after_commit(self):

        m = maintain_schema("schema2", self.session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                self.session.commit()
                self.session.execute("select 1")
                m.new_tx_listener.assert_called_once_with(
                    self.session, mock.ANY, mock.ANY)
                assert self.session.execute.call_count == 3
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema1"
            assert m.new_tx_listener.call_count == 1

    def test_maintain_schema_after_rollback(self):

        m = maintain_schema("schema2", self.session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                self.session.rollback()
                self.session.execute("select 1")
                m.new_tx_listener.assert_called_once_with(
                    self.session, mock.ANY, mock.ANY)
                assert self.session.execute.call_count == 3
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema1"
            assert m.new_tx_listener.call_count == 1

    def test_maintain_schema_nested(self):
        """Test doubly nested `maintain_schema`"""

        m_level1 = maintain_schema("schema2", self.session)
        m_level2 = maintain_schema("schema3", self.session)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                assert self.session.execute.call_count == 1
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

                with m_level2:
                    assert self.session.execute.call_count == 2
                    assert str(self.session.execute.call_args[0][0]) == \
                           str(SetSchema("schema3"))
                    assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema3"

                # must be reverted to level1
                assert self.session.execute.call_count == 3
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema1"
            assert m_level1.new_tx_listener.called == False
            assert m_level2.new_tx_listener.called == False

    def test_maintain_schema_multiple_sessions_nested(self, mock_session2):
        """Test doubly nested `maintain_schema` but with different sessions"""

        m_level1 = maintain_schema("schema2", self.session)
        m_level2 = maintain_schema("schema3", mock_session2)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                assert self.session.execute.call_count == 1
                assert str(self.session.execute.call_args[0][0]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

                assert mock_session2.execute.called is False
                assert SchemaContextManager._get_schema_stack(mock_session2).top is None

                with m_level2:
                    assert mock_session2.execute.call_count == 2
                    assert str(mock_session2.execute.call_args_list[-2][0][0]) == \
                           str(GetSchema())
                    assert str(mock_session2.execute.call_args_list[-1][0][0]) == \
                           str(SetSchema("schema3"))

                    assert SchemaContextManager._get_schema_stack(mock_session2).top[0] == "schema3"

                # level2 must be reverted to the original
                assert SchemaContextManager._get_schema_stack(mock_session2).top[0] == "default_schema"

                # level1 should be unchanged
                assert self.session.execute.call_count == 1
                assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema2"

            # level1 must be reverted to the original
            assert SchemaContextManager._get_schema_stack(self.session).top[0] == "schema1"

            assert m_level1.new_tx_listener.called is False
            assert m_level2.new_tx_listener.called is False

    def test_rollback_state(self, Model):
        """Test that exiting the context manager with a session in partial rollback
        will not cause a new exception when trying to execute the schema reset.
        """

        m = Model(id=2)
        self.session.add(m)
        self.session.commit()

        # should raise FlushError caused by the invalid INSERT, and not the
        # InvalidRequestError: This Session's transaction has been rolled back ...
        with pytest.raises(FlushError):
            with maintain_schema("schema1", self.session):
                m2 = Model(id=2)
                self.session.add(m2)
                self.session.flush()

    def test_handled_exception_not_swallowed(self):
        """Test that when the contect manager exit is called while an exception
        is being handled and the exit fails, the original exception is still
        raised
        """
        class ExecuteError(Exception): pass
        class BusinessLogicError(Exception): pass

        patcher = mock.patch.object(self.session, "execute", side_effect=ExecuteError)

        with pytest.raises(BusinessLogicError):
            with maintain_schema("schema1", self.session):
                patcher.start()
                raise BusinessLogicError

        patcher.stop()

    def test_exception_raised(self):
        """Test that when no exception is being handled, and the contect manager
        exit failed, the exception is raised
        """
        class ExecuteError(Exception): pass

        patcher = mock.patch.object(self.session, "execute", side_effect=ExecuteError)

        with pytest.raises(ExecuteError):
            with maintain_schema("schema1", self.session):
                patcher.start()
                a = 2

        patcher.stop()


def test_retrieve_default_schema(mock_session):
    """Test that if no schema is already found the existing value will be
    retrieved by the db
    """
    m = maintain_schema("schema2", mock_session)
    assert SchemaContextManager._get_schema_stack(mock_session).top is None
    with mock.patch.object(
            m, "new_tx_listener", side_effect=m.new_tx_listener):
        with m:
            assert mock_session.execute.call_count == 2
            assert str(mock_session.execute.call_args_list[-2][0][0]) == \
                   str(GetSchema())

            assert m.new_tx_listener.called == False
            assert SchemaContextManager._get_schema_stack(mock_session)[-2][0] == "default_schema"

        # must be reverted to the original
        assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "default_schema"
        assert m.new_tx_listener.call_count == 0


@pytest.fixture
def scoped_ses(engine):
    d = {"id": 0}
    def session_id():
        global _id
        d["id"] += 1
        return d["id"]

    def create_session():
        return _mock_session(engine)
    return scoped_session(create_session, scopefunc=session_id)

def test_get_schema_stack_scoped_session(scoped_ses):
    """Test that scoped_session proxies are correctly mapped to different
    schema stacks"""
    assert scoped_ses() is not scoped_ses()

    stack = SchemaContextManager._get_schema_stack(scoped_ses)
    stack2 = SchemaContextManager._get_schema_stack(scoped_ses)
    assert stack is not stack2
