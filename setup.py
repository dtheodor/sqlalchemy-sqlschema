# -*- coding: utf-8 -*-
"""
SQLAlchemy-SQLSchema
-----------------------

Provides a contextmanager with the capability to set the SQL schema for a
connection or session. The schema will persist through multiple transactions.
"""
import sys
import os
from setuptools import setup

if sys.version_info < (2, 6):
    raise Exception("SQLAlchemy-SQLSchema requires Python 2.6 or higher.")

# Hard linking doesn't work inside VirtualBox shared folders. This means that
# you can't use tox in a directory that is being shared with Vagrant,
# since tox relies on `python setup.py sdist` which uses hard links. As a
# workaround, disable hard-linking if setup.py is a descendant of /vagrant.
# See
# https://stackoverflow.com/questions/7719380/python-setup-py-sdist-error-operation-not-permitted
# for more details.
if os.path.abspath(__file__).split(os.path.sep)[1] == 'vagrant':
    del os.link

tests_require = ["pytest>=2.6", "mock>=1.0"]
tests_require_postgres = ["psycopg2>=2.6"]
tests_require_all = tests_require + tests_require_postgres

setup(
    name="SQLAlchemy-SQLSchema",
    version="0.1",
    packages=["sqlalchemy_sqlschema"],
    author="Dimitris Theodorou",
    author_email="dimitris.theodorou@gmail.com",
    url='http://github.com/dtheodor/sqlalchemy_sqlschema',
    license="MIT",
    description='Set the SQL schema over multiple transactions.',
    long_description=__doc__,
    classifiers=[
        #'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        #'Programming Language :: Python :: 3',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    install_requires=["sqlalchemy>=0.9"],
    tests_require=tests_require,
    extras_require={
        'docs': ["Sphinx>=1.3.1", "alabaster>=0.7.4"],
        'tests': tests_require,
        'tests_postgres': tests_require_postgres,
        'tests_all': tests_require_all
    }
)
