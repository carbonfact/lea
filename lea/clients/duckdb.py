from __future__ import annotations

import duckdb

from lea import views

from .base import Client


class DuckDB(Client):

    def __init__(self, path: str, schema: str, username: str, console):
        self.path = path
        self._schema = schema
        self.username = username
        self.console = console
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

    def prepare(self):
        self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.console.log(f"Created schema {self.schema}")

    def _create_python(self, view: views.PythonView):
        self._load_python(view)
        self.con.sql(f"CREATE OR REPLACE TABLE {self.schema}.{view.dunder_name} AS SELECT * FROM dataframe")

    def _create_sql(self, view: views.SQLView):
        query = view.query.replace(f"{self._schema}.", f"{self.schema}.")
        self.con.sql(f"CREATE OR REPLACE TABLE {self.schema}.{view.dunder_name} AS ({query})")

    def _load_sql(self, view: views.SQLView):
        raise NotImplementedError()

    def delete_view(self, view: views.View):
        raise NotImplementedError()

    def list_existing_view_names(self) -> list[tuple[str, str]]:
        results = duckdb.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
        return [
            (r["table_schema"], r["table_name"])
            for r in results.to_dict(orient="records")
        ]

    def get_diff_summary(self, origin: str, destination: str):
        raise NotImplementedError()
