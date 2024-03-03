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
        import duckdb

        if path.startswith("md:"):
            path = f"{path}_{username}" if username is not None else path
        else:
            path = pathlib.Path(path)
            if username is not None:
                path = (path.parent / f"{path.stem}_{username}{path.suffix}").absolute()
        self.path = path
        self.username = username
        self.wap_mode = wap_mode
        self.con = duckdb.connect(str(self.path))

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.DUCKDB

    @property
    def is_motherduck(self):
        return self.path.startswith("md:")

    def prepare(self, views):
        schemas = set(view.schema for view in views)
        for schema in schemas:
            self.con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            console.log(f"Created schema {schema}")

    def teardown(self):
        os.remove(self.path)

    def materialize_sql_view(self, view):
        self.con.sql(f"CREATE OR REPLACE TABLE {view.table_reference} AS ({view.query})")

    def materialize_sql_view_incremental(self, view, incremental_field_name):
        self.con.sql(f"""
        INSERT INTO {view.table_reference}
        SELECT *
        FROM ({view.query})
        WHERE {incremental_field_name} > (SELECT MAX({incremental_field_name}) FROM {view.table_reference})
        """)

    def materialize_python_view(self, view):
        dataframe = self.read_python_view(view)
        self.con.sql(f"CREATE OR REPLACE TABLE {view.table_reference} AS SELECT * FROM dataframe")

    def delete_table_reference(self, table_reference):
        self.con.sql(f"DROP TABLE IF EXISTS {table_reference}")

    def read_sql(self, query: str) -> pd.DataFrame:
        return self.con.cursor().sql(query).df()

    def list_tables(self) -> pd.DataFrame:
        return self.read_sql(f"""
        SELECT
            '{self.path.stem}' || '.' || schema_name || '.' || table_name AS table_reference,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        """)

    def list_columns(self) -> pd.DataFrame:
        return self.read_sql(f"""
        SELECT
            '{self.path.stem}' || '.' || table_schema || '.' || table_name AS table_reference,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        """)

    def _view_key_to_table_reference(self, view_key: tuple[str], with_context: bool) -> str:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._view_key_to_table_reference(("schema", "table"), with_context=False)
        'schema.table'

        >>> client._view_key_to_table_reference(("schema", "subschema", "table"), with_context=False)
        'schema.subschema__table'

        """
        schema, *leftover = view_key
        table_reference = f"{schema}.{lea._SEP.join(leftover)}"
        if with_context:
            if self.username:
                table_reference = f"{self.path.stem}.{table_reference}"
            if self.wap_mode:
                table_reference = f"{table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX}"
        return table_reference

    def _table_reference_to_view_key(self, table_reference: str) -> tuple[str]:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._table_reference_to_view_key("schema.table")
        ('schema', 'table')

        >>> client._table_reference_to_view_key("schema.subschema__table")
        ('schema', 'subschema', 'table')

        """
        database, leftover = table_reference.split(".", 1)
        if database == self.path.stem:
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
        try:
            # Concatenate all the statements into one string and execute them
            sql = "\n".join(f"{statement};" for statement in statements)
            self.con.execute(f"BEGIN TRANSACTION; {sql} COMMIT;")
        except duckdb.ProgrammingError as e:
            # Make sure to rollback if there's an error
            self.con.execute("ROLLBACK")
            raise e
