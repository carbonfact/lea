from __future__ import annotations

import os

import duckdb
import pandas as pd

import lea

from .base import Client


class DuckDB(Client):
    def __init__(self, path: str, username: str | None):
        self.path = path
        self.username = username
        self.con = duckdb.connect(self.path)

    @property
    def sqlglot_dialect(self):
        return "duckdb"

    def prepare(self, views, console):
        schemas = set(
            f"{view.schema}_{self.username}" if self.username else view.schema for view in views
        )
        for schema in schemas:
            self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            console.log(f"Created schema {schema}")

    def _create_python(self, view: lea.views.PythonView):
        dataframe = self._load_python(view)  # noqa: F841
        self.con.sql(
            f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS SELECT * FROM dataframe"
        )

    def _create_sql(self, view: lea.views.SQLView):
        query = view.query
        if self.username:
            for schema in {schema for schema, *_ in view.dependencies}:
                query = query.replace(f"{schema}.", f"{schema}_{self.username}.")
        self.con.sql(f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS ({query})")

    def _load_sql(self, view: lea.views.SQLView):
        query = view.query
        if self.username:
            for schema in {schema for schema, *_ in view.dependencies}:
                query = query.replace(f"{schema}.", f"{schema}_{self.username}.")
        return self.con.cursor().sql(query).df()

    def delete_view(self, view: lea.views.View):
        self.con.sql(f"DROP TABLE IF EXISTS {self._make_view_path(view)}")

    def teardown(self):
        os.remove(self.path)

    def list_existing_view_names(self) -> list[tuple[str, str]]:
        results = duckdb.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
        return [(r["table_schema"], r["table_name"]) for r in results.to_dict(orient="records")]

    def get_columns(self, schema=None) -> pd.DataFrame:
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
        schema = f"{schema}_{self.username}" if self.username else schema
        return f"{schema}.{lea._SEP.join(leftover)}"

    def make_test_unique_column(self, view: lea.views.View, column: str) -> str:
        schema, *leftover = view.key
        return f"""
        SELECT {column}, COUNT(*) AS n
        FROM {f"{schema}.{lea._SEP.join(leftover)}"}
        GROUP BY {column}
        HAVING n > 1
        """
