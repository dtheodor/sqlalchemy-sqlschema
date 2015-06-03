# -*- coding: utf-8 -*-
"""
Tests run against a PostgreSQL database.
"""
import mock
import pytest
from sqlalchemy_sqlschema.sql import set_schema, get_schema

from sqlalchemy_sqlschema import maintain_schema

@pytest.yield_fixture(scope="session")
def test_schemas(pg_engine):
    """Create a couple of schemas to use in tests, drop them when done"""
    schema_names = ("test_schema_1", "test_schema_2")
    for schema in schema_names:
        pg_engine.execute("CREATE SCHEMA IF NOT EXISTS {}".format(schema))
    yield schema_names
    for schema in schema_names:
        pg_engine.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(schema))

@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestPostgresSqlStatementExecution(object):

    def test_get_schema(self, pg_session, pg_test_schema, test_schemas):
        assert pg_session.execute(get_schema()).scalar() == \
               pg_session.execute("show search_path").scalar() == \
               pg_test_schema

        pg_session.execute("set search_path to {}".format(test_schemas[0]))

        assert pg_session.execute(get_schema()).scalar() == \
               pg_session.execute("show search_path").scalar() == \
               test_schemas[0]


    def test_set_schema(self, pg_session, test_schemas):
        pg_session.execute(set_schema(test_schemas[1]))
        assert pg_session.execute("show search_path").scalar() == \
               test_schemas[1]

        pg_session.execute(set_schema(test_schemas[1] + ",public"))
        assert pg_session.execute("show search_path").scalar() == \
               test_schemas[1] + ", public"


@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestSessionMaintainSchemaPostgres(object):

    def test_maintain_schema(self, pg_session, pg_test_schema, test_schemas):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        m = maintain_schema(test_schemas[0] + ",public", pg_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert pg_session.execute("show search_path").scalar() == \
                       test_schemas[0] + ", public"
                assert m.new_tx_listener.called == False

            # must be reverted to the original
            assert pg_session.execute("show search_path").scalar() == pg_test_schema
            assert m.new_tx_listener.call_count == 0

    def test_maintain_schema_after_commit(self, pg_session, pg_test_schema, test_schemas):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        m = maintain_schema(test_schemas[0] + ",public", pg_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert pg_session.execute("show search_path").scalar() == \
                       test_schemas[0] + ", public"
                pg_session.commit()
                assert pg_session.execute("show search_path").scalar() == \
                       test_schemas[0] + ", public"
                m.new_tx_listener.assert_called_once_with(
                    pg_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert pg_session.execute("show search_path").scalar() == pg_test_schema
            assert m.new_tx_listener.call_count == 1

    def test_maintain_schema_after_rollback(self, pg_session, pg_test_schema, test_schemas):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        m = maintain_schema(test_schemas[0] + ",public", pg_session)
        with mock.patch.object(
                m, "new_tx_listener", side_effect=m.new_tx_listener):
            with m:
                assert pg_session.execute("show search_path").scalar() == \
                       test_schemas[0] + ", public"
                pg_session.rollback()
                assert pg_session.execute("show search_path").scalar() == \
                       test_schemas[0] + ", public"
                m.new_tx_listener.assert_called_once_with(
                    pg_session, mock.ANY, mock.ANY)

            # must be reverted to the original
            assert pg_session.execute("show search_path").scalar() == pg_test_schema
            assert m.new_tx_listener.call_count == 1

@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestMaintainSchemaNestedPostgres(object):

    def assert_the_schema_abides(self, mgr, pg_session, expected_schema, called_so_far=0):
        """Test that the search_path equals the ``expected_schema`` now and
        after a rollback and a commit.

        Also verify the new transaction listener has been called the right
        amount of times, which is once after a rollback and once after a commit.
        """
        assert pg_session.execute("show search_path").scalar() == expected_schema
        assert mgr.new_tx_listener.called == bool(called_so_far)
        pg_session.commit()
        assert pg_session.execute("show search_path").scalar() == expected_schema
        mgr.new_tx_listener.assert_called_with(pg_session, mock.ANY, mock.ANY)
        assert mgr.new_tx_listener.call_count == 1 + called_so_far
        pg_session.rollback()
        assert pg_session.execute("show search_path").scalar() == expected_schema
        mgr.new_tx_listener.assert_called_with(pg_session, mock.ANY, mock.ANY)
        assert mgr.new_tx_listener.call_count == 2 + called_so_far

    def test_maintain_schema_nested(self, pg_session, pg_test_schema, test_schemas):
        """Test doubly nested `maintain_schema`"""

        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        m_level1 = maintain_schema(test_schemas[0] + ",public", pg_session)
        m_level2 = maintain_schema(test_schemas[1] + ",public", pg_session)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                self.assert_the_schema_abides(
                    m_level1, pg_session, test_schemas[0] + ", public")

                with m_level2:
                    self.assert_the_schema_abides(
                        m_level2, pg_session, test_schemas[1] + ", public")

                # must be reverted to level1
                self.assert_the_schema_abides(
                    m_level1, pg_session, test_schemas[0] + ", public", called_so_far=2)

            # must be reverted to the original
            assert pg_session.execute("show search_path").scalar() == pg_test_schema
