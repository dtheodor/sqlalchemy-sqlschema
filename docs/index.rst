.. SQLAlchemy-SQLSchema documentation master file, created by
   sphinx-quickstart on Sun May 31 21:42:23 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SQLAlchemy-SQLSchema
====================

.. toctree::
   :maxdepth: 2
.. currentmodule:: sqlalchemy_sqlschema

This library provides the capability to specify the active
`SQL Schema <http://www.postgresql.org/docs/9.4/static/ddl-schemas.html>`_
through the :func:`maintain_schema` context manager/decorator, which will
maintain the selected schema until its exit. Multiple transactions with commits
and/or rollbacks may take place inside the context manager without affecting the
active SQL schema.

The main use case for such functionality is when schemas need to be changed
dynamically. This is often needed when using schemas to implement multi-tenancy,
i.e. when identical tables are placed in multiple different schemas but an
end-user has access to only one of them. This allows to maximize re-use of code
and database operations while providing isolation.

SQL schemas are not supported by all databases. PostgreSQL is one of them and
supported by this library. An implementation for
`Microsoft SQL <https://msdn.microsoft.com/en-us/library/ms189462.aspx>`_ and
`Oracle <http://docs.oracle.com/cd/B19306_01/server.102/b14231/general.htm#i1107794>`_
is yet to be developed. Any contributions appreciated!

.. contents::
   :local:
   :backlinks: none

Usage
-----

Assuming you have hold of a :class:`~sqlalchemy.orm.session.Session`, you can
use :func:`maintain_schema` as a context manager:

.. code-block:: python

    from sqlalchemy_sqlschema import maintain_schema

    with maintain_schema("my_schema", session):
        schema = session.execute("show search_path").scalar()
        assert schema == "my_schema"

        # the following query needs to find a `my_schema.my_model_table` table
        session.query(MyModel)

        # a rollback still maintains the schema
        session.rollback()
        assert session.execute("show search_path").scalar() == "my_schema"


Or as a decorator:

.. code-block:: python

    from sqlalchemy_sqlschema import maintain_schema

    @maintain_schema("my_schema", session)
    def query_data():
        assert session.execute("show search_path").scalar() == "my_schema"
        return session.query(MyModel).all()

Implementation
--------------

The SQL schema is set by using dialect-specific SQL clauses, of which only the
`PostgreSQL implementations <http://www.postgresql.org/docs/9.4/static/ddl-schemas.html#DDL-SCHEMAS-PATH>`_
are implemented. SQL Alchemy events are used to set
the schema again right after a new transaction is started (which is needed since
a rollback will reset the schema to the value it had before the transaction
start).



API
---

.. autofunction:: sqlalchemy_sqlschema.maintain_schema

.. autofunction:: sqlalchemy_sqlschema.sql.get_schema

.. autofunction:: sqlalchemy_sqlschema.sql.set_schema



Web Application Example
-----------------------

A useful scenario is when a web application redirects different users to
different SQL schemas.

First, we need a way to know the SQL schema per user. In this case, it is a
column on the user table directly:

.. code-block:: python

    from sqlalchemy import Column, Integer, String
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        id = Column(Integer, primary_key=True)
        schema = Column(String)


Let's setup our web application to set the right SQL schema. We
are using Flask and Flask-Login to get access to the ``current_user``:

.. code-block:: python

    from flask import Flask, jsonify
    from flask_login import current_user

    app = Flask(__name__)

    @app.route("/api/data")
    def data():
        with maintain_schema(current_user.schema, session)
            data = session.query(MyModel).all()
            return jsonify(data=data)


In the example above, the table of ``MyModel`` needs to exist in the selected
schema otherwise the query will fail. Setting the schema also means that the
user is "locked" in that schema and cannot see any other tables in
different schemas.

The above can be achieved more succinctly with a decorator:

.. code-block:: python

    from decorator import decorator

    @decorator
    def set_user_schema(f, *args, **kwargs):
        """Call `maintain_schema` with the current_user's schema."""
        with maintain_schema(current_user.schema, session):
            return f(*args, **kwargs)

    @app.route("/api/data")
    @set_user_schema
    def data():
        data = session.query(Data).all()
        return jsonify(data=data)


The same decorator could be applied in a :class:`~flask.views.View`:

.. code-block:: python

    from flask import View

    class SecretView(View):
        methods = ['GET']
        decorators = [set_user_schema]

        def dispatch_request(self):
            # ...

