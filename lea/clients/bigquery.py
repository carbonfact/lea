from __future__ import annotations

import os

import pandas as pd
import rich.console
import sqlglot

import lea

from .base import Client

# HACK
console = rich.console.Console()


class BigQuery(Client):
    def __init__(self, credentials, location, project_id, dataset_name, username):
        self.credentials = credentials
        self.project_id = project_id
        self.location = location
        self._dataset_name = dataset_name
        self.username = username

    @property
    def dataset_name(self):
        return f"{self._dataset_name}_{self.username}" if self.username else self._dataset_name

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.BIGQUERY

    @property
    def client(self):
        from google.cloud import bigquery

        return bigquery.Client(credentials=self.credentials)

    def prepare(self, views):
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(
            project=self.project_id, dataset_id=self.dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        console.log(f"Created dataset {dataset.dataset_id}")

    def teardown(self):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
        console.log(f"Deleted dataset {dataset.dataset_id}")

    def _materialize_sql_query(self, view_key: tuple[str], query: str, wap_mode=False):
        table_reference = self._view_key_to_table_reference(view_key, wap_mode=wap_mode)
        schema, table_reference = table_reference.split(".", 1)
        job = self.client.create_job(
            {
                "query": {
                    "query": query,
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_name,
                        "tableId": table_reference,
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                },
                "labels": {
                    "job_dataset": self.dataset_name,
                    "job_schema": schema,
                    "job_table": table_reference.replace(f"{lea._SEP}{lea._WAP_MODE_SUFFIX}", ""),
                    "job_username": self.username,
                    "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                },
            }
        )
        job.result()

    def _materialize_pandas_dataframe(self, view_key: tuple[str], dataframe: pd.DataFrame, wap_mode=False):
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            dataframe,
            f"{self.project_id}.{self._view_key_to_table_reference(view_key, wap_mode=wap_mode)}",
            job_config=job_config,
        )
        job.result()

    def delete_view_key(self, view_key: tuple[str]):
        table_reference = self._view_key_to_table_reference(view_key, with_username=True)
        self.client.delete_table(f"{self.project_id}.{table_reference}")

    def _read_sql_view(self, view: lea.views.View) -> pd.DataFrame:
        return pd.read_gbq(view.query, credentials=self.client._credentials)

    def list_tables(self):
        query = f"""
        SELECT
            FORMAT('%s.%s', '{self.dataset_name}', table_id) AS table_reference,
            row_count AS n_rows,
            size_bytes AS n_bytes
        FROM {self.dataset_name}.__TABLES__
        """
        view = lea.views.GenericSQLView(query=query, sqlglot_dialect=self.sqlglot_dialect)
        return self._read_sql_view(view)

    def list_columns(self) -> pd.DataFrame:
        query = f"""
        SELECT
            FORMAT('%s.%s', table_schema, table_name) AS table_reference,
            column_name AS column,
            data_type AS type
        FROM {self.dataset_name}.INFORMATION_SCHEMA.COLUMNS
        """
        view = lea.views.GenericSQLView(query=query, sqlglot_dialect=self.sqlglot_dialect)
        return self._read_sql_view(view)

    def _view_key_to_table_reference(self, view_key: tuple[str], with_username=False, wap_mode=False) -> str:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     project_id="project",
        ...     dataset_name="dataset",
        ...     username="max"
        ... )

        >>> client._view_key_to_table_reference(("schema", "table"))
        'dataset.schema__table'

        >>> client._view_key_to_table_reference(("schema", "subschema", "table"))
        'dataset.schema__subschema__table'

        >>> client._view_key_to_table_reference(("schema", "table"))
        'dataset.schema__table'

        >>> client._view_key_to_table_reference(("schema", "table"), with_username=True)
        'dataset_max.schema__table'

        >>> client._view_key_to_table_reference(("schema", "table"), with_username=True, wap_mode=True)
        'dataset_max.schema__table__LEA_WAP'

        """
        if with_username:
            table_reference = f"{self.dataset_name}.{lea._SEP.join(view_key)}"
        else:
            table_reference = f"{self._dataset_name}.{lea._SEP.join(view_key)}"
        if wap_mode:
            table_reference = f"{table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX}"
        return table_reference

    def _table_reference_to_view_key(self, table_reference: str) -> tuple[str]:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     project_id="project",
        ...     dataset_name="dataset",
        ...     username="max"
        ... )

        >>> client._table_reference_to_view_key("dataset.schema__table")
        ('schema', 'table')

        >>> client._table_reference_to_view_key("dataset.schema__subschema__table")
        ('schema', 'subschema', 'table')

        >>> client._table_reference_to_view_key("external_dataset.schema__subschema__table")
        ('external_dataset', 'schema', 'subschema', 'table')

        >>> client._table_reference_to_view_key("dataset_max.schema__table")
        ('schema', 'table')

        """
        dataset, leftover = tuple(table_reference.split(".", 1))
        if dataset in {self._dataset_name, self.dataset_name}:
            return tuple(leftover.split(lea._SEP))
        return (dataset, *leftover.split(lea._SEP))

    def switch_for_wap_mode(self, table_references):
        statements = []
        for table_reference in table_references:
            # Drop the existing table if it exists
            statements.append(f"DROP TABLE IF EXISTS {table_reference}")
            # Rename the WAP table to the original table name
            table_reference_without_schema = table_reference.split(".", 1)[1]
            statements.append(
                f"ALTER TABLE {table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX} RENAME TO {table_reference_without_schema}"
            )

        sql = '\n'.join(f'{statement};' for statement in statements)
        sql = f"""
        BEGIN

            BEGIN TRANSACTION;
            {sql}
            COMMIT TRANSACTION;

        EXCEPTION WHEN ERROR THEN
            -- Roll back the transaction inside the exception handler.
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
        END;
        """

        self.client.query(sql).result()
