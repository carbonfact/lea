from __future__ import annotations

import os
import pathlib

import pandas as pd
import rich.console
import sqlglot

import lea

from .base import Client

# HACK
console = rich.console.Console()


class DuckDB(Client):
    def __init__(self, path: str, username: str | None = None):
        import duckdb

        if path.startswith("md:"):
            path = f"{path}_{username}" if username is not None else path
        else:
            path = pathlib.Path(path)
            if username is not None:
                path = (path.parent / f"{path.stem}_{username}{path.suffix}").absolute()
        self.path = path
        self.username = username
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

    def _materialize_pandas_dataframe(
        self, view_key: tuple[str], dataframe: pd.DataFrame, wap_mode=False
    ):
        table_reference = self._view_key_to_table_reference(view_key=view_key, wap_mode=wap_mode)
        self.con.sql(f"CREATE OR REPLACE TABLE {table_reference} AS SELECT * FROM dataframe")

    def _materialize_sql_query(self, view_key: tuple[str], query: str, wap_mode=False):
        table_reference = self._view_key_to_table_reference(view_key=view_key, wap_mode=wap_mode)
        self.con.sql(f"CREATE OR REPLACE TABLE {table_reference} AS ({query})")

    def _read_sql_view(self, view: lea.views.SQLView):
        query = view.query
        return self.con.cursor().sql(query).df()

    def delete_view_key(self, view_key: tuple[str]):
        table_reference = self._view_key_to_table_reference(view_key)
        self.con.sql(f"DROP TABLE IF EXISTS {table_reference}")

    def teardown(self):
        os.remove(self.path)

    def list_tables(self) -> pd.DataFrame:
        query = f"""
        SELECT
            '{self.path.stem}' || '.' || schema_name || '.' || table_name AS table_reference,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        """
        return self.con.sql(query).df()

    def list_columns(self) -> pd.DataFrame:
        query = f"""
        SELECT
            '{self.path.stem}' || '.' || table_schema || '.' || table_name AS table_reference,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        """
        return self.con.sql(query).df()

    def _view_key_to_table_reference(
        self, view_key: tuple[str], with_username=False, wap_mode=False
    ) -> str:
        """

        >>> client = DuckDB(path=":memory:", username=None)

        >>> client._view_key_to_table_reference(("schema", "table"))
        'schema.table'

        >>> client._view_key_to_table_reference(("schema", "subschema", "table"))
        'schema.subschema__table'

        """
        schema, *leftover = view_key
        table_reference = f"{schema}.{lea._SEP.join(leftover)}"
        if with_username and self.username:
            table_reference = f"{self.path.stem}.{table_reference}"
        if wap_mode:
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
        return (schema, *leftover.split(lea._SEP))

    def switch_for_wap_mode(self, table_references):
        statements = []
        for table_reference in table_references:
            # Drop the existing table if it exists
            statements.append(f"DROP TABLE IF EXISTS {table_reference};")
            # Rename the WAP table to the original table name
            table_reference_without_schema = table_reference.split(".", 1)[1]
            statements.append(
                f"ALTER TABLE {table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX} RENAME TO {table_reference_without_schema};"
            )
        try:
            # Concatenate all the statements into one string and execute them
            sql = "\n".join(f"{statement};" for statement in statements)
            self.con.execute(f"BEGIN TRANSACTION; {sql} COMMIT;")
        except Exception as e:
            # Make sure to rollback if there's an error
            self.con.execute("ROLLBACK")
            raise e
