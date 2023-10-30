from __future__ import annotations

import os
import pathlib

import duckdb
import pandas as pd
import sqlglot

import lea

from .base import Client


class DuckDB(Client):
    def __init__(self, path: str, username: str | None):
        if username is not None:
            _path = pathlib.Path(path)
            path = str((_path.parent / f"{_path.stem}_{username}{_path.suffix}").absolute())
        self.path = path
        self.username = username
        self.con = duckdb.connect(self.path)

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
            f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS SELECT * FROM dataframe"
        )

    def _create_sql_view(self, view: lea.views.SQLView):
        query = view.query
        self.con.sql(f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS ({query})")

    def _load_sql_view(self, view: lea.views.SQLView):
        query = view.query
        return self.con.cursor().sql(query).df()

    def delete_view(self, view: lea.views.View):
        self.con.sql(f"DROP TABLE IF EXISTS {self._make_view_path(view)}")

    def teardown(self):
        os.remove(self.path)

    def list_existing_view_names(self) -> list[tuple[str, str]]:
        query = """
            SELECT
                CASE
                    WHEN POSITION('_' IN table_schema) > 0
                    THEN SUBSTRING(table_schema FROM 1 FOR POSITION('_' IN table_schema) - 1)
                    ELSE table_schema
                END table_schema,
                table_name
            FROM information_schema.tables

        """
        return [
            (r["table_schema"], r["table_name"])
            for r in self.con.sql(query).df().to_dict(orient="records")
        ]

    def get_tables(self):
        query = """
        SELECT
            schema_name || '.' || table_name AS view_name,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        """
        return self.con.sql(query).df()

    def get_columns(self) -> pd.DataFrame:
        query = """
        SELECT
             table_schema || '.' || table_name AS view_name,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        """
        return self.con.sql(query).df()

    def _make_view_path(self, view: lea.views.View) -> str:
        schema, *leftover = view.key
        return f"{schema}.{lea._SEP.join(leftover)}"

    def make_test_unique_column(self, view: lea.views.View, column: str) -> str:
        schema, *leftover = view.key
        return f"""
        SELECT {column}, COUNT(*) AS n
        FROM {f"{schema}.{lea._SEP.join(leftover)}"}
        GROUP BY {column}
        HAVING n > 1
        """
