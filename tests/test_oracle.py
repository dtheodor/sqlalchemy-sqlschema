# -*- coding: utf-8 -*-
"""
Tests to run against an Oracle database.
"""
import pytest

from sqlalchemy_sqlschema.sql import set_schema, get_schema
from sqlalchemy_sqlschema import maintain_schema

@pytest.mark.usefixtures("maintain_oracle_test_schema")
class TestOracleSqlStatementExecution(object):

    def test_get_schema(self, oracle_session, oracle_schema1, oracle_schema2):
        assert oracle_session.execute(get_schema()).scalar() == \
               oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
               oracle_schema1

        oracle_session.execute("ALTER SESSION SET CURRENT_SCHEMA = {}".format(oracle_schema2))

        assert oracle_session.execute(get_schema()).scalar() == \
               oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
               oracle_schema2


    def test_set_schema(self, oracle_session, oracle_schema2, oracle_schema3):
        oracle_session.execute(set_schema(oracle_schema2))
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
               oracle_schema2

        oracle_session.execute(set_schema(oracle_schema3))
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
               oracle_schema3

@pytest.mark.usefixtures("maintain_oracle_test_schema")
class TestSessionMaintainSchemaOracle(object):

    def test_maintain_schema(self, oracle_session, oracle_schema1, oracle_schema2):
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

        with maintain_schema(oracle_schema2, oracle_session):
            assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
                   oracle_schema2

        # must be reverted to the original
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

    def test_maintain_schema_after_commit(self, oracle_session, oracle_schema1, oracle_schema2):
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

        with maintain_schema(oracle_schema2, oracle_session):
            assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
                   oracle_schema2
            oracle_session.commit()
            assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
                   oracle_schema2

        # must be reverted to the original
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

    def test_maintain_schema_after_rollback(self, oracle_session, oracle_schema1, oracle_schema2):
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

        with maintain_schema(oracle_schema2, oracle_session):
            assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
                   oracle_schema2
            oracle_session.rollback()
            assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == \
                   oracle_schema2

        # must be reverted to the original
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

@pytest.mark.usefixtures("maintain_oracle_test_schema")
class TestMaintainSchemaNestedOracle(object):

    def assert_the_schema_abides(self, oracle_session, expected_schema):
        """Test that the search_path equals the ``expected_schema`` now and
        after a rollback and a commit.

        Also verify the new transaction listener has been called the right
        amount of times, which is once after a rollback and once after a commit.
        """
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == expected_schema
        oracle_session.commit()
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == expected_schema
        oracle_session.rollback()
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == expected_schema


    def test_maintain_schema_nested(self, oracle_session, oracle_schema1, oracle_schema2, oracle_schema3):
        """Test doubly nested `maintain_schema`"""

        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1

        with maintain_schema(oracle_schema2, oracle_session):
            self.assert_the_schema_abides(oracle_session, oracle_schema2)

            with maintain_schema(oracle_schema3, oracle_session):
                self.assert_the_schema_abides(oracle_session, oracle_schema3)

            # must be reverted to level1
            self.assert_the_schema_abides(oracle_session, oracle_schema2)

        # must be reverted to the original
        assert oracle_session.execute("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").scalar() == oracle_schema1
