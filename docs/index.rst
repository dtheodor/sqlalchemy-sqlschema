.. SQLAlchemy-SQLSchema documentation master file, created by
   sphinx-quickstart on Sun May 31 21:42:23 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SQLAlchemy-SQLSchema
================================================

.. toctree::
   :maxdepth: 2
.. currentmodule:: sqlalchemy_sqlschema

SQLAlchemy-SQLSchema's main purpose is to provide the :func:`maintain_schema`
context manager which will set the
`SQL schema <http://www.postgresql.org/docs/9.4/static/ddl-schemas.html>`_
and maintain it until the context manager exit. Multiple transactions with
commits and rollbacks may take place inside the context manager, as well as
nested context managers setting a different SQL schema.

The main use case for such functionality is when different schemas are used to
implement multi-tenancy, i.e. when identical tables are placed in multiple
different schemas, and end-users have access only to one of them. This restricts
each end-user only to data he should be having access to, while maximizing
re-use of code and database operations.

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
use :func:`maintain_schema` as follows::

    from sqlalchemy_sqlschema import maintain_schema

    with maintain_schema("my_schema", session):
        schema = session.execute("show search_path").scalar()
        assert schema == "my_schema"

        session.query(MyModel) # MyModel needs to exist in "my_schema"

        session.rollback()
        # a rollback still maintains the schema
        assert session.execute("show search_path").scalar() == "my_schema"


API
---

.. autofunction:: sqlalchemy_sqlschema.maintain_schema

.. autofunction:: sqlalchemy_sqlschema.sql.get_schema

.. autofunction:: sqlalchemy_sqlschema.sql.set_schema



Web Application Example
-----------------------

A useful scenario is a web application that redirects different users to
different SQL schemas.

First, we need a way to know the SQL schema per user. In this case, it is a
column on the user table directly::

    from sqlalchemy import Column, Integer, String
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        id = Column(Integer, primary_key=True)
        schema = Column(String)


Let's setup our web application to set the right SQL schema. In this case we
are using Flask and Flask-Login to get access to the ``current_user``::


    from flask import Flask, jsonify
    from flask_login import current_user

    app = Flask(__name__)

    @app.route("/api/data")
    def data():
        with maintain_schema(current_user.schema, session)
            # the Data table needs to exist in the schema that has been set
            # by the current_user, otherwise this query will fail
            data = session.query(Data).all()
            return jsonify(data=data)


More succinctly, this can be achieved by using a decorator::


    def maintain_user_schema(f):
        """Call `maintain_schema` on the current_user's schema."""
        def decorator(*args, **kwargs):
            with maintain_schema(current_user.schema, session):
                return f(*args, **kwargs)
        return decorator

    @app.route("/api/data")
    @maintain_user_schema
    def data():
        data = session.query(Data).all()
        return jsonify(data=data)


The same decorator could be applied in a :class:`~flask.views.View`::

    class SecretView(View):
        methods = ['GET']
        decorators = [maintain_user_schema]

        def dispatch_request(self):
            # ...

