# -*- coding: utf-8 -*-
"""
Tests to run against a PostgreSQL database.
"""
try:
    from unittest import mock
except:
    import mock
import pytest

from sqlalchemy_sqlschema.sql import set_schema, get_schema
from sqlalchemy_sqlschema import maintain_schema


@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestPostgresSqlStatementExecution(object):

    def test_get_schema(self, pg_session, pg_test_schema):
        assert pg_session.execute(get_schema()).scalar() == \
               pg_session.execute("show search_path").scalar() == \
               pg_test_schema

        pg_session.execute("set search_path to {}".format("test_schema_1"))

        assert pg_session.execute(get_schema()).scalar() == \
               pg_session.execute("show search_path").scalar() == \
               "test_schema_1"


    def test_set_schema(self, pg_session):
        pg_session.execute(set_schema("test_schema_2"))
        assert pg_session.execute("show search_path").scalar() == \
               "test_schema_2"

        pg_session.execute(set_schema("test_schema_2,public"))
        assert pg_session.execute("show search_path").scalar() == \
               "test_schema_2, public"


@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestSessionMaintainSchemaPostgres(object):

    def test_maintain_schema(self, pg_session, pg_test_schema):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        with maintain_schema("test_schema_1,public", pg_session):
            assert pg_session.execute("show search_path").scalar() == \
                   "test_schema_1, public"

        # must be reverted to the original
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

    def test_maintain_schema_after_commit(self, pg_session, pg_test_schema):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        with maintain_schema("test_schema_1,public", pg_session):
            assert pg_session.execute("show search_path").scalar() == \
                   "test_schema_1, public"
            pg_session.commit()
            assert pg_session.execute("show search_path").scalar() == \
                   "test_schema_1, public"

        # must be reverted to the original
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

    def test_maintain_schema_after_rollback(self, pg_session, pg_test_schema):
        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        with maintain_schema("test_schema_1,public", pg_session):
            assert pg_session.execute("show search_path").scalar() == \
                   "test_schema_1, public"
            pg_session.rollback()
            assert pg_session.execute("show search_path").scalar() == \
                   "test_schema_1, public"

        # must be reverted to the original
        assert pg_session.execute("show search_path").scalar() == pg_test_schema


@pytest.mark.usefixtures("maintain_pg_test_schema")
class TestMaintainSchemaNestedPostgres(object):

    def assert_the_schema_abides(self, mgr, pg_session, expected_schema, called_so_far=0):
        """Test that the search_path equals the ``expected_schema`` now and
        after a rollback and a commit.

        Also verify the new transaction listener has been called the right
        amount of times, which is once after a rollback and once after a commit.

        TODO: remove mocks and asserts after the non-postgres test can trigger
        new tranasctions with rollback/commit
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

    def test_maintain_schema_nested(self, pg_session, pg_test_schema):
        """Test doubly nested `maintain_schema`"""

        assert pg_session.execute("show search_path").scalar() == pg_test_schema

        m_level1 = maintain_schema("test_schema_1,public", pg_session)
        m_level2 = maintain_schema("test_schema_2,public", pg_session)
        with mock.patch.object(
                m_level1, "new_tx_listener",
                side_effect=m_level1.new_tx_listener), \
            mock.patch.object(
                m_level2, "new_tx_listener",
                side_effect=m_level2.new_tx_listener):
            with m_level1:
                self.assert_the_schema_abides(
                    m_level1, pg_session, "test_schema_1, public")

                with m_level2:
                    self.assert_the_schema_abides(
                        m_level2, pg_session, "test_schema_2, public")

                # must be reverted to level1
                self.assert_the_schema_abides(
                    m_level1, pg_session, "test_schema_1, public", called_so_far=2)

            # must be reverted to the original
            assert pg_session.execute("show search_path").scalar() == pg_test_schema
