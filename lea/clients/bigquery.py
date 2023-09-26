from __future__ import annotations

import os
import textwrap

import pandas as pd

import lea

from .base import Client


class BigQuery(Client):
    def __init__(self, credentials, location, project_id, dataset_name, username):
        from google.cloud import bigquery

        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(credentials=credentials)
        self._dataset_name = dataset_name
        self.username = username

    def prepare(self, console):
        dataset = self.create_dataset()
        console.log(f"Created dataset {dataset.dataset_id}")

    @property
    def sqlglot_dialect(self):
        return "bigquery"

    @property
    def dataset_name(self):
        return (
            f"{self._dataset_name}_{self.username}"
            if self.username
            else self._dataset_name
        )

    def create_dataset(self):
        from google.cloud import bigquery
        dataset_ref = bigquery.DatasetReference(project=self.project_id, dataset_id=self.dataset_name)
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
                        "tableId": f"{view.schema}__{view.name}" if view.schema else view.name,
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE"
                },
                "labels": {
                    "job_dataset": self.dataset_name,
                    "job_schema": view.schema,
                    "job_table": f"{view.schema}__{view.name}" if view.schema else view.name,
                    "job_username": self.username,
                    "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                },
            }
        )

    def _create_sql(self, view: lea.views.SQLView):
        job = self._make_job(view)
        job.result()

    def _create_python(self, view: lea.views.PythonView):
        from google.cloud import bigquery

        dataframe = self._load_python(view)

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            dataframe,
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}",
            job_config=job_config,
        )
        job.result()

    def _load_sql(self, view: lea.views.SQLView) -> pd.DataFrame:
        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")
        return pd.read_gbq(query, credentials=self.client._credentials)

    def list_existing_view_names(self):
        return [
            table.table_id.split("__", 1) for table in self.client.list_tables(self.dataset_name)
        ]

    def delete_view(self, view: lea.views.View):
        self.client.delete_table(
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}"
        )

    def get_columns(self, schema=None) -> pd.DataFrame:
        schema = schema or self.dataset_name
        query = f"""
        SELECT
            table_name AS table,
            column_name AS column,
            data_type AS type
        FROM {schema}.INFORMATION_SCHEMA.COLUMNS
        """
        return self._load_sql(lea.views.GenericSQLView(schema=None, name=None, query=query))

    def yield_unit_tests(self, view, view_columns):
        column_comments = view.extract_comments(columns=view_columns, dialect=self.sqlglot_dialect)

        for column, comment_block in column_comments.items():
            for comment in comment_block:
                if "@" in comment.text:
                    if comment.text == "@UNIQUE":
                        yield lea.views.GenericSQLView(
                            schema="tests",
                            name=f"{view.schema}.{view.name}.{column}@UNIQUE",
                            query=textwrap.dedent(
                                f"""
                                SELECT {column}, COUNT(*) AS n
                                FROM {self.dataset_name}.{view.schema}__{view.name}
                                GROUP BY {column}
                                HAVING n > 1
                                """
                            ),
                        )
                    else:
                        raise ValueError(f"Unhandled tag: {comment.text}")
