from __future__ import annotations

import os
import pathlib

import duckdb
import pandas as pd
import rich.console
import sqlglot

import lea

from .base import Client

# HACK
console = rich.console.Console()


class DuckDB(Client):
    def __init__(self, path: str, username: str | None = None, wap_mode: bool = False):
        self.path = path
        self.username = username
        self.wap_mode = wap_mode

        path_ = pathlib.Path(path)
        if path.startswith("md:"):
            path_ = pathlib.Path(f"{path}_{username}" if username is not None else path)
        elif username is not None:
            path_ = pathlib.Path(path)
            path_ = path_.parent / f"{path_.stem}_{username}{path_.suffix}"
        self.path_ = path_

    def __repr__(self):
        return ("Running on DuckDB\n" f"{self.path=}\n" f"{self.username=}").replace("self.", "")

    def _make_con(self):
        import duckdb

        return (
            duckdb.connect(self.path)
            if self.path.startswith(":")  # e.g. ":memory:", ":memory:username", ":default:"
            else duckdb.connect(str(self.path_.absolute()))
        )

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.DUCKDB

    @property
    def is_motherduck(self):
        return self.path.startswith("md:")

    def prepare(self, views):
        schemas = set(view.schema for view in views)
        with self._make_con() as con:
            for schema in schemas:
                con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                console.log(f"Created schema {schema}")

    def teardown(self):
        os.remove(self.path)

    def materialize_sql_view(self, view):
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        with self._make_con() as con:
            con.sql(f"CREATE OR REPLACE TABLE {table_reference} AS ({view.query})")

    def _materialize_pandas_dataframe(self, dataframe, table_reference):
        with self._make_con() as con:
            con.sql(f"CREATE OR REPLACE TABLE {table_reference} AS SELECT * FROM dataframe")

    def materialize_python_view(self, view):
        dataframe = self.read_python_view(view)  # noqa: F841
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        self._materialize_pandas_dataframe(dataframe, table_reference)

    def materialize_json_view(self, view):
        dataframe = pd.read_json(view.path)  # noqa: F841
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        self._materialize_pandas_dataframe(dataframe, table_reference)

    def delete_table_reference(self, table_reference):
        with self._make_con() as con:
            con.sql(f"DROP TABLE IF EXISTS {table_reference}")

    def read_sql(self, query: str) -> pd.DataFrame:
        with self._make_con() as con:
            return con.sql(query).df()

    def list_tables(self) -> pd.DataFrame:
        return self.read_sql(
            f"""
        SELECT
            '{self.path_.stem}' || '.' || schema_name || '.' || table_name AS table_reference,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        """
        )

    def list_columns(self) -> pd.DataFrame:
        return self.read_sql(
            f"""
        SELECT
            '{self.path_.stem}' || '.' || table_schema || '.' || table_name AS table_reference,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        """
        )

    def _view_key_to_table_reference(
        self, view_key: tuple[str, ...], with_context: bool, with_project_id=False
    ) -> str:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._view_key_to_table_reference(("schema", "table"), with_context=False)
        'schema.table'

        >>> client._view_key_to_table_reference(("schema", "subschema", "table"), with_context=False)
        'schema.subschema__table'

        """
        leftover: list[str] = []
        schema, *leftover = view_key
        table_reference = f"{schema}.{lea._SEP.join(leftover)}"
        if with_context:
            if self.username:
                table_reference = f"{self.path_.stem}.{table_reference}"
            if self.wap_mode:
                table_reference = f"{table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX}"
        return table_reference

    def _table_reference_to_view_key(self, table_reference: str) -> tuple[str, ...]:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._table_reference_to_view_key("schema.table")
        ('schema', 'table')

        >>> client._table_reference_to_view_key("schema.subschema__table")
        ('schema', 'subschema', 'table')

        """
        database, leftover = table_reference.split(".", 1)
        if database == self.path_.stem:
            schema, leftover = leftover.split(".", 1)
        else:
            schema = database
        key = (schema, *leftover.split(lea._SEP))
        if key[-1] == lea._WAP_MODE_SUFFIX:
            key = key[:-1]
        return key

    def switch_for_wap_mode(self, view_keys: list[tuple[str]]):
        statements = []
        for view_key in view_keys:
            table_reference = self._view_key_to_table_reference(view_key, with_context=True)
            table_reference_without_wap = table_reference.replace(
                lea._SEP + lea._WAP_MODE_SUFFIX, ""
            )
            statements.append(f"DROP TABLE IF EXISTS {table_reference_without_wap}")
            statements.append(
                f"ALTER TABLE {table_reference.split('.', 1)[1]} RENAME TO {table_reference_without_wap.split('.', 2)[2]}"
            )
        with self._make_con() as con:
            try:
                # Concatenate all the statements into one string and execute them
                sql = "\n".join(f"{statement};" for statement in statements)
                con.execute(f"BEGIN TRANSACTION; {sql} COMMIT;")
            except duckdb.ProgrammingError as e:
                # Make sure to rollback if there's an error
                con.execute("ROLLBACK")
                raise e
