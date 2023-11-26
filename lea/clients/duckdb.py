from __future__ import annotations

import os
import pathlib

import pandas as pd
import sqlglot

import lea

from .base import AssertionTag, Client


class DuckDB(Client):
    def __init__(self, path: str, username: str | None = None):
        import duckdb

        if path.startswith("md:"):
            path = f"{path}_{username}" if username is not None else path
        else:
            if username is not None:
                _path = pathlib.Path(path)
                path = str((_path.parent / f"{_path.stem}_{username}{_path.suffix}").absolute())
        self.path = path
        self.username = username
        self.con = duckdb.connect(self.path)

    @property
    def is_motherduck(self):
        return self.path.startswith("md:")

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.DUCKDB

    def prepare(self, views, console):
        schemas = set(view.schema for view in views)
        for schema in schemas:
            self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            console.log(f"Created schema {schema}")

    def _create_python_view(self, view: lea.views.PythonView):
        dataframe = self._load_python_view(view)  # noqa: F841
        self.con.sql(
            f"CREATE OR REPLACE TABLE {self._key_to_reference(view.key)} AS SELECT * FROM dataframe"
        )

    def _create_sql_view(self, view: lea.views.SQLView):
        query = view.query
        self.con.sql(f"CREATE OR REPLACE TABLE {self._key_to_reference(view.key)} AS ({query})")

    def _load_sql_view(self, view: lea.views.SQLView):
        query = view.query
        return self.con.cursor().sql(query).df()

    def delete_table_reference(self, table_reference: str):
        self.con.sql(f"DROP TABLE IF EXISTS {table_reference}")

    def teardown(self):
        os.remove(self.path)

    def list_tables(self) -> pd.DataFrame:
        query = """
        SELECT
            schema_name || '.' || table_name AS table_reference,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        """
        return self.con.sql(query).df()

    def list_columns(self) -> pd.DataFrame:
        query = """
        SELECT
            table_schema || '.' || table_name AS table_reference,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        """
        return self.con.sql(query).df()

    def _key_to_reference(self, view_key: tuple[str]) -> str:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._key_to_reference(("schema", "table"))
        'schema.table'

        >>> client._key_to_reference(("schema", "subschema", "table"))
        'schema.subschema__table'

        """
        schema, *leftover = view_key
        return f"{schema}.{lea._SEP.join(leftover)}"

    def _reference_to_key(self, table_reference: str) -> tuple[str]:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._reference_to_key("schema.table")
        ('schema', 'table')

        >>> client._reference_to_key("schema.subschema__table")
        ('schema', 'subschema', 'table')

        """
        schema, leftover = table_reference.split(".", 1)
        return (schema, *leftover.split(lea._SEP))
