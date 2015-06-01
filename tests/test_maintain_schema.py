# -*- coding: utf-8 -*-
"""
Created by @dtheodor at 2015-05-31

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


@pytest.mark.usefixtures("reset_schema_for_session")
class TestSessionMaintainSchema(object):

    def test_maintain_schema(self, session):
        assert session.execute("show search_path").scalar() == 'public'

        m = maintain_schema("test_schema,public", session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert session.execute("show search_path").scalar() == \
                       'test_schema, public'
                assert m.new_tx_listener.called == False

            # must be reverted to the original
            assert session.execute("show search_path").scalar() == 'public'
            assert m.new_tx_listener.call_count == 0

    def test_maintain_schema_after_commit(self, session):
        assert session.execute("show search_path").scalar() == 'public'

        m = maintain_schema("test_schema,public", session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert session.execute("show search_path").scalar() == \
                       'test_schema, public'
                session.commit()
                assert session.execute("show search_path").scalar() == \
                       'test_schema, public'
                m.new_tx_listener.assert_called_once_with(
                    session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert session.execute("show search_path").scalar() == 'public'
            assert m.new_tx_listener.call_count == 1

    def test_maintain_schema_after_rollback(self, session):
        assert session.execute("show search_path").scalar() == 'public'

        m = maintain_schema("test_schema,public", session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert session.execute("show search_path").scalar() == \
                       'test_schema, public'
                session.rollback()
                assert session.execute("show search_path").scalar() == \
                       'test_schema, public'
                m.new_tx_listener.assert_called_once_with(
                    session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert session.execute("show search_path").scalar() == 'public'
            assert m.new_tx_listener.call_count == 1

@pytest.mark.usefixtures("reset_schema_for_session")
class TestMaintainSchemaNested(object):

    def assert_the_schema_abides(self, mgr, session, expected_schema, called_so_far=0):
        """Test that the search_path equals the ``expected_schema`` now and
        after a rollback and a commit.

        Also verify the new transaction listener has been called the right
        amount of times, which is once after a rollback and once after a commit.
        """
        assert session.execute("show search_path").scalar() == expected_schema
        assert mgr.new_tx_listener.called == bool(called_so_far)
        session.commit()
        assert session.execute("show search_path").scalar() == expected_schema
        mgr.new_tx_listener.assert_called_with(session, mock.ANY, mock.ANY)
        assert mgr.new_tx_listener.call_count == 1 + called_so_far
        session.rollback()
        assert session.execute("show search_path").scalar() == expected_schema
        mgr.new_tx_listener.assert_called_with(session, mock.ANY, mock.ANY)
        assert mgr.new_tx_listener.call_count == 2 + called_so_far

    def test_maintain_schema_nested(self, session):
        """Test doubly nested `maintain_schema`"""

        assert session.execute("show search_path").scalar() == 'public'

        m_level1 = maintain_schema("schema_level1,public", session)
        m_level2 = maintain_schema("schema_level2,public", session)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                self.assert_the_schema_abides(
                    m_level1, session, 'schema_level1, public')

                with m_level2:
                    self.assert_the_schema_abides(
                        m_level2, session, 'schema_level2, public')

                # must be reverted to level1
                self.assert_the_schema_abides(
                    m_level1, session, 'schema_level1, public', called_so_far=2)

            # must be reverted to the original
            assert session.execute("show search_path").scalar() == 'public'
