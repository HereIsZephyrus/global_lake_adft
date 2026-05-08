"""Domain port interfaces for lakesource backends.

Each module defines a focused Protocol that Postgres and Parquet backends
implement as repositories.  New code should depend on these ports, not on
concrete backend modules.
"""
