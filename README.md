##SQLAlchemy-SQLSchema

Provides a context manager to dynamically modify the active
[SQL Schema](http://www.postgresql.org/docs/9.4/static/ddl-schemas.html#DDL-SCHEMAS-PATH).
Works for PostgreSQL. Useful for implementing multi-tenancy.

http://sqlalchemy-sqlschema.readthedocs.org

###Usage

As a context manager:

```python
from sqlalchemy_sqlschema import maintain_schema

with maintain_schema("my_schema", session):
    schema = session.execute("show search_path").scalar()
    assert schema == "my_schema"

    # the following query needs to find a `my_schema.my_model_table` table
    session.query(MyModel) 

    # a rollback still maintains the schema
    session.rollback()
    assert session.execute("show search_path").scalar() == "my_schema"
```

As a decorator:

```python
from sqlalchemy_sqlschema import maintain_schema

@maintain_schema("my_schema", session):
def query_data():
    assert session.execute("show search_path").scalar() == "my_schema"
    return session.query(MyModel).all() 
```

An example of dynamically changing the SQL schema based on the current user in
a Flask application. Assumes that the user has a `schema` attribute that points
to the desired SQL schema.

```python
from flask import Flask, jsonify
from flask_login import current_user

app = Flask(__name__)

@app.route("/api/data")
def data():
    with maintain_schema(current_user.schema, session)
        data = session.query(MyModel).all()
        return jsonify(data=data)
```


### Tests

You need to create your own `tests/test.config` file that will provide a URL to
a database to be used for the tests. Then you can run the tests by invoking
`PYTHONPATH=. py.test tests/` in the repository root.
