from __future__ import annotations

import os

import pandas as pd
import sqlglot

import lea

from .base import Client


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

    def prepare(self, views, console):
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(
            project=self.project_id, dataset_id=self.dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        console.log(f"Created dataset {dataset.dataset_id}")

    def teardown(self, console):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
        console.log(f"Deleted dataset {dataset.dataset_id}")

    def _materialize_sql_query(self, view_key: tuple[str], query: str):
        table_reference = self._view_key_to_table_reference(view_key)
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
                    "job_table": table_reference,
                    "job_username": self.username,
                    "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                },
            }
        )
        job.result()

    def _materialize_pandas_dataframe(self, view_key: tuple[str], dataframe: pd.DataFrame):
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            dataframe,
            f"{self.project_id}.{self._view_key_to_table_reference(view_key)}",
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
            FORMAT('%s.%s', table_schema, table_name) AS table_reference,
            total_rows AS n_rows,
            total_logical_bytes AS n_bytes
        FROM `region-{self.location.lower()}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
        WHERE table_schema = '{self.dataset_name}'
        AND total_rows > 0
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

    def _view_key_to_table_reference(self, view_key: tuple[str], with_username=False) -> str:
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

        """
        if with_username:
            return f"{self.dataset_name}.{lea._SEP.join(view_key)}"
        return f"{self._dataset_name}.{lea._SEP.join(view_key)}"

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
