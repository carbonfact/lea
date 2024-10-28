from __future__ import annotations

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
        self.database = self._get_database_name(path, username)

        path_ = pathlib.Path(self.path)
        if self.username:
            path_ = path_.parent / f"{path_.stem}_{username}{path_.suffix}"
        self.path_ = path_

    def _get_database_name(self, path: str, username: str | None) -> str:
        # remove prefixes/suffixes regardless of path type
        # we can't use Path with md as it always expects the name of the database you want to create/query/drop...
        if self.is_motherduck:
            base = path.removeprefix("md:")
        else:
            base = pathlib.Path(path.strip(":")).stem
            for suffix in (".db", ".duckdb", ".ddb"):
                base = base.removesuffix(suffix)

        return f"{base}_{username}" if username else base

    def __repr__(self):
        return ("Running on DuckDB\n" f"{self.path=}\n" f"{self.username=}").replace("self.", "")

    def _make_con(self):
        # instantitiate motherduck connections without connecting to a specific database to allow lea to switch between prod & dev db
        return duckdb.connect("md:" if self.is_motherduck else str(self.path_))

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.DUCKDB

    @property
    def is_motherduck(self):
        return self.path.startswith("md:")

    def prepare(self, views):
        schemas = set(view.schema for view in views)
        with self._make_con() as con:
            # this creates the md database that lea will write to (can be prod or dev db depending on --production flag)
            if self.is_motherduck:
                con.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                console.log(f"Created database {self.database}")

            for schema in schemas:
                con.sql(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{schema}")
                console.log(f"Created schema {schema}")

    def teardown(self):
        if self.is_motherduck:
            with self._make_con() as con:
                # md expects a database name so not paths
                con.sql(f"DROP DATABASE IF EXISTS {self.database}")
        else:
            pathlib.Path(self.path_).unlink(missing_ok=True)

        console.log(f"Deleted database {self.database}")

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
            # we need to specify the database because md returns ALL tables from ALL databases by default
            f"""
        SELECT
            '{self.database}' || '.' || schema_name || '.' || table_name AS table_reference,
            estimated_size AS n_rows,  -- TODO: Figure out how to get the exact number
            estimated_size AS n_bytes  -- TODO: Figure out how to get this
        FROM duckdb_tables()
        WHERE database_name = '{self.database}'
        """
        )

    def list_columns(self) -> pd.DataFrame:
        # we need to specify the database because md returns ALL tables from ALL databases by default
        return self.read_sql(
            f"""
        SELECT
            '{self.database}' || '.' || table_schema || '.' || table_name AS table_reference,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        WHERE table_catalog = '{self.database}'
        """
        )

    def _view_key_to_table_reference(
        self, view_key: tuple[str, ...], with_context: bool = True, with_project_id=False
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
            # if self.username:
            table_reference = f"{self.database}.{table_reference}"
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
        if database == self.database:
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
                f"ALTER TABLE {table_reference} RENAME TO {table_reference_without_wap.split('.', 2)[2]}"
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
