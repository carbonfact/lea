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
        dataset = self.create_dataset()
        console.log(f"Created dataset {dataset.dataset_id}")

    def create_dataset(self):
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(
            project=self.project_id, dataset_id=self.dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        return dataset

    def teardown(self):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

    def _make_job(self, view: lea.views.SQLView):
        query = view.query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")

        return self.client.create_job(
            {
                "query": {
                    "query": query,
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_name,
                        "tableId": f"{self._key_to_reference(view.key).split('.', 1)[1]}",
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                },
                "labels": {
                    "job_dataset": self.dataset_name,
                    "job_schema": view.schema,
                    "job_table": f"{self._key_to_reference(view.key).split('.', 1)[1]}",
                    "job_username": self.username,
                    "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                },
            }
        )

    def _create_sql_view(self, view: lea.views.SQLView):
        job = self._make_job(view)
        job.result()

    def _create_python_view(self, view: lea.views.PythonView):
        from google.cloud import bigquery

        dataframe = self._load_python_view(view)

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            dataframe,
            f"{self.project_id}.{self._key_to_reference(view.key)}",
            job_config=job_config,
        )
        job.result()

    def _render_view_query(self, view: lea.views.SQLView) -> str:
        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")
        return query

    def _load_sql_view(self, view: lea.views.SQLView) -> pd.DataFrame:
        query = self._render_view_query(view)
        return pd.read_gbq(query, credentials=self.client._credentials)

    def delete_table_reference(self, table_reference: str):
        self.client.delete_table(f"{self.project_id}.{table_reference}")

    def list_tables(self):
        query = f"""
        SELECT
            FORMAT('%s.%s', table_schema, table_name) AS table_reference,
            total_rows AS n_rows,
            total_logical_bytes AS n_bytes
        FROM `region-{self.location.lower()}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
        WHERE table_schema = '{self.dataset_name}'
        """
        view = lea.views.GenericSQLView(query=query, sqlglot_dialect=self.sqlglot_dialect)
        return self._load_sql_view(view)

    def list_columns(self) -> pd.DataFrame:
        query = f"""
        SELECT
            FORMAT('%s.%s', table_schema, table_name) AS table_reference,
            column_name AS column,
            data_type AS type
        FROM {self.dataset_name}.INFORMATION_SCHEMA.COLUMNS
        """
        view = lea.views.GenericSQLView(query=query, sqlglot_dialect=self.sqlglot_dialect)
        columns = self._load_sql_view(view)
        return columns

    def _key_to_reference(self, view_key: tuple[str]) -> str:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     project_id="project",
        ...     dataset_name="dataset",
        ...     username=None
        ... )


        >>> client._key_to_reference(("schema", "table"))
        'dataset.schema__table'

        >>> client._key_to_reference(("schema", "subschema", "table"))
        'dataset.schema__subschema__table'

        """
        return f"{self.dataset_name}.{lea._SEP.join(view_key)}"

    def _reference_to_key(self, table_reference: str) -> tuple[str]:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     project_id="project",
        ...     dataset_name="dataset",
        ...     username=None
        ... )

        >>> client._reference_to_key("dataset.schema__table")
        ('schema', 'table')

        >>> client._reference_to_key("dataset.schema__subschema__table")
        ('schema', 'subschema', 'table')

        """
        return tuple(table_reference.split(".", 1)[1].split(lea._SEP))
