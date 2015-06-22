# -*- coding: utf-8 -*-
"""
Test maintain_schema contextmanager
"""
import mock
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

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
def set_previous_schema():
    SchemaContextManager._get_schema_stack().push(("schema1", None))
    yield
    SchemaContextManager._get_schema_stack().pop()


def test_retrieve_default_schema(mock_session):
    m = maintain_schema("schema2", mock_session)
    assert SchemaContextManager._get_schema_stack().top is None
    with mock.patch.object(
            m, "new_tx_listener", side_effect=m.new_tx_listener):
        with m:
            assert mock_session.execute.call_count == 2
            assert str(mock_session._execute_calls[-2]) == \
                   str(GetSchema())
            assert m.new_tx_listener.called == False
            assert SchemaContextManager._get_schema_stack()[-2][0] == "default_schema"

        # must be reverted to the original
        assert SchemaContextManager._get_schema_stack().top[0] == "default_schema"
        assert m.new_tx_listener.call_count == 0

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
                assert m.new_tx_listener.called == False
                assert SchemaContextManager._get_schema_stack().top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack().top[0] == "schema1"
            assert m.new_tx_listener.call_count == 0

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
                assert SchemaContextManager._get_schema_stack().top[0] == "schema2"

                m.new_tx_listener.assert_called_once_with(
                    mock_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack().top[0] == "schema1"
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
                assert SchemaContextManager._get_schema_stack().top[0] == "schema2"

                m.new_tx_listener.assert_called_once_with(
                    mock_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack().top[0] == "schema1"
            assert m.new_tx_listener.call_count == 1

@pytest.mark.usefixtures("set_previous_schema")
class TestMaintainSchemaNested(object):

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
                assert m_level1.new_tx_listener.called == False
                assert SchemaContextManager._get_schema_stack().top[0] == "schema2"

                with m_level2:
                    assert mock_session.execute.call_count == 2
                    assert str(mock_session._execute_calls[-1]) == \
                           str(SetSchema("schema3"))
                    assert m_level1.new_tx_listener.called == False
                    assert SchemaContextManager._get_schema_stack().top[0] == "schema3"

                # must be reverted to level1
                assert mock_session.execute.call_count == 3
                assert str(mock_session._execute_calls[-1]) == \
                       str(SetSchema("schema2"))
                assert m_level1.new_tx_listener.called == False
                assert SchemaContextManager._get_schema_stack().top[0] == "schema2"

            # must be reverted to the original
            assert SchemaContextManager._get_schema_stack().top[0] == "schema1"
            assert m_level1.new_tx_listener.call_count == 0

