# -*- coding: utf-8 -*-
"""
SQLAlchemy-SQLSchema
-----------------------

Provides a contextmanager with the capability to set the SQL schema for a
connection or session. The schema will persist through multiple transactions.
"""
# pylint: disable=wildcard-import
from .maintain_schema import *

__version__ = 0.1
