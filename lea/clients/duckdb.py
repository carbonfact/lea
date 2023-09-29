from __future__ import annotations

import os

import duckdb
import pandas as pd

from lea import views

from .base import Client


class DuckDB(Client):

    def __init__(self, path: str, schema: str, username: str):
        self.path = path
        self._schema = schema
        self.username = username
        self.con = duckdb.connect(self.path)

    @property
    def sqlglot_dialect(self):
        return "duckdb"

    @property
    def schema(self):
        return (
            f"{self._schema}_{self.username}"
            if self.username
            else self._schema
        )

    def prepare(self, console):
        self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        console.log(f"Created schema {self.schema}")

    def _create_python(self, view: views.PythonView):
        dataframe = self._load_python(view)  # noqa: F841
        self.con.sql(f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS SELECT * FROM dataframe")

    def _create_sql(self, view: views.SQLView):
        query = view.query.replace(f"{self._schema}.", f"{self.schema}.")
        self.con.sql(f"CREATE OR REPLACE TABLE {self._make_view_path(view)} AS ({query})")

    def _load_sql(self, view: views.SQLView):
        query = view.query
        if self.username:
            query = query.replace(f"{self._schema}.", f"{self.schema}.")
        return self.con.cursor().sql(query).df()

    def delete_view(self, view: views.View):
        self.con.sql(f"DROP TABLE IF EXISTS {self._make_view_path(view)}")

    def teardown(self):
        os.remove(self.path)

    def list_existing_view_names(self) -> list[tuple[str, str]]:
        results = duckdb.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
        return [
            (r["table_schema"], r["table_name"])
            for r in results.to_dict(orient="records")
        ]

    def get_columns(self, schema=None) -> pd.DataFrame:
        schema = schema or self.schema
        query = f"""
        SELECT
            table_name AS table,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        """
        return self.con.sql(query).df()

    def _make_view_path(self, view: views.View) -> str:
        return f"{self.schema}.{view.dunder_name}"

    def make_test_unique_column(self, view: views.View, column: str) -> str:
        return f"""
        SELECT {column}, COUNT(*) AS n
        FROM {self._make_view_path(view)}
        GROUP BY {column}
        HAVING n > 1
        """
