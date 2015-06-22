# -*- coding: utf-8 -*-
"""
Test maintain_schema contextmanager
"""
import mock
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session

from sqlalchemy_sqlschema import maintain_schema
from sqlalchemy_sqlschema.maintain_schema import Stack, SchemaContextManager
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

@pytest.fixture(scope="session")
def mock_engine():
    def dump(sql, *multiparams, **params):
        try:
            stmt = sql.compile(dialect=engine.dialect)
        except:
            stmt = sql
        engine._last_stmt = stmt
    engine = create_engine('sqlite://', strategy='mock', executor=dump)
    return engine

def _mock_session():
    session = Session(mock_engine)
    session._execute_calls = []

    class GetSchemaResult:
        def scalar(self):
            return "default_schema"

    def execute(stmt, *args, **kwargs):
        session._execute_calls.append(stmt)
        if isinstance(stmt, GetSchema):
            return GetSchemaResult()
        else:
            return "executed"
    patcher = mock.patch.object(session, "execute", side_effect=execute)
    patcher.start()
    return session


@pytest.fixture
def mock_session():
    return _mock_session()

@pytest.fixture
def mock_session2():
    return _mock_session()

@pytest.yield_fixture
def set_previous_schema(mock_session):
    SchemaContextManager._get_schema_stack(mock_session).push(("schema1", None))
    yield
    SchemaContextManager._get_schema_stack(mock_session).pop()


@pytest.mark.usefixtures("set_previous_schema")
class TestSessionMaintainSchema(object):

    def test_maintain_schema(self, mock_session):
        m = maintain_schema("schema2", mock_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert mock_session.execute.call_count == 1
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema1"
            assert m.new_tx_listener.called == False

    @pytest.mark.skipif(True, reason="until a new transaction can be mocked")
    def test_maintain_schema_after_commit(self, mock_session):

        m = maintain_schema("schema2", mock_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert mock_session.execute.call_count == 2
                mock_session.commit()
                assert mock_session.execute.call_count == 3
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

                m.new_tx_listener.assert_called_once_with(
                    mock_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema1"
            assert m.new_tx_listener.call_count == 1

    @pytest.mark.skipif(True, reason="until a new transaction can be mocked")
    def test_maintain_schema_after_rollback(self, mock_session):

        m = maintain_schema("schema2", mock_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                mock_session.rollback()
                assert mock_session.execute.call_count == 3
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

                m.new_tx_listener.assert_called_once_with(
                    mock_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema1"
            assert m.new_tx_listener.call_count == 1

    def test_maintain_schema_nested(self, mock_session):
        """Test doubly nested `maintain_schema`"""

        m_level1 = maintain_schema("schema2", mock_session)
        m_level2 = maintain_schema("schema3", mock_session)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                assert mock_session.execute.call_count == 1
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

                with m_level2:
                    assert mock_session.execute.call_count == 2
                    assert str(mock_session._execute_calls[-1]) == \
                           str(SetSchema("schema3"))
                    assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema3"

                # must be reverted to level1
                assert mock_session.execute.call_count == 3
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema1"
            assert m_level1.new_tx_listener.called == False
            assert m_level2.new_tx_listener.called == False

    def test_maintain_schema_multiple_sessions_nested(self, mock_session, mock_session2):
        """Test doubly nested `maintain_schema` but with different sessions"""

        m_level1 = maintain_schema("schema2", mock_session)
        m_level2 = maintain_schema("schema3", mock_session2)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                assert mock_session.execute.call_count == 1
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

                assert mock_session2.execute.called is False
                assert SchemaContextManager._get_schema_stack(mock_session2).top is None

                with m_level2:
                    assert mock_session2.execute.call_count == 2
                    assert str(mock_session2._execute_calls[-2]) == \
                           str(GetSchema())
                    assert str(mock_session2._execute_calls[-1]) == \
                           str(SetSchema("schema3"))
                    assert SchemaContextManager._get_schema_stack(mock_session2).top[0] == "schema3"

                # level2 must be reverted to the original
                assert SchemaContextManager._get_schema_stack(mock_session2).top[0] == "default_schema"

                # level1 should be unchanged
                assert mock_session.execute.call_count == 1
                assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema2"

            # level1 must be reverted to the original
            assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "schema1"

            assert m_level1.new_tx_listener.called is False
            assert m_level2.new_tx_listener.called is False


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
            assert str(mock_session._execute_calls[-2]) == \
                   str(GetSchema())
            assert m.new_tx_listener.called == False
            assert SchemaContextManager._get_schema_stack(mock_session)[-2][0] == "default_schema"

        # must be reverted to the original
        assert SchemaContextManager._get_schema_stack(mock_session).top[0] == "default_schema"
        assert m.new_tx_listener.call_count == 0


@pytest.fixture
def scoped_ses():
    d = {"id": 0}
    def session_id():
        global _id
        d["id"] += 1
        return d["id"]
    return scoped_session(_mock_session, scopefunc=session_id)

def test_get_schema_stack_scoped_session(scoped_ses):
    """Test that scoped_session proxies are correctly mapped to different
    schema stacks"""
    assert scoped_ses() is not scoped_ses()

    stack = SchemaContextManager._get_schema_stack(scoped_ses)
    stack2 = SchemaContextManager._get_schema_stack(scoped_ses)
    assert stack is not stack2
