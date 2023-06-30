from __future__ import annotations

import abc
import importlib
import os

import pandas as pd

from . import views


class Client(abc.ABC):
    @abc.abstractmethod
    def _create_sql(self, view: views.SQLView):
        ...

    @abc.abstractmethod
    def _create_python(self, view: views.PythonView):
        ...

    def create(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._create_sql(view)
        elif isinstance(view, views.PythonView):
            return self._create_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def _load_sql(self, view: views.SQLView):
        ...

    def _load_python(self, view: views.PythonView):
        # HACK
        mod = importlib.import_module("views")
        output = getattr(mod, view.name).main()
        return output

    def load(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._load_sql(view)
        elif isinstance(view, views.PythonView):
            return self._load_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def list_existing(self, schema: str) -> list[str]:
        ...

    @abc.abstractmethod
    def delete(self, view_name: str):
        ...


class BigQuery(Client):
    def __init__(self, credentials, location, project_id, dataset_name, username):
        from google.cloud import bigquery

        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(credentials=credentials)
        self._dataset_name = dataset_name
        self.username = username

    @property
    def dataset_name(self):
        return (
            f"{self._dataset_name}_{self.username}"
            if self.username
            else self._dataset_name
        )

    def create_dataset(self):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset.dataset_id}")

    def delete_dataset(self):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

    def _make_job(self, view: views.SQLView):
        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")

        return self.client.create_job(
            {
                "query": {
                    "query": query,
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_name,
                        "tableId": f"{view.schema}__{view.name}".lstrip("_"),
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                },
                "labels": {
                    "job_dataset": self.dataset_name,
                    "job_schema": view.schema,
                    "job_table": f"{view.schema}__{view.name}".lstrip("_"),
                    "job_username": self.username,
                    "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                },
            }
        )

    def _create_sql(self, view: views.SQLView):
        job = self._make_job(view)
        job.result()

    def _create_python(self, view: views.PythonView):
        from google.cloud import bigquery

        output = self._load_python(view)

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            output,
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}",
            job_config=job_config,
        )
        job.result()

    def _load_sql(self, view: views.SQLView) -> pd.DataFrame:
        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")
        return pd.read_gbq(query, credentials=self.client._credentials)

    def list_existing(self):
        return [
            table.table_id.split("__", 1)
            for table in self.client.list_tables(self.dataset_name)
        ]

    def delete(self, view: views.View):
        self.client.delete_table(
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}"
        )

    def get_diff_summary(self, origin_dataset: str, destination_dataset: str):
        # TODO: this is creating a view, whereas it should just provide a result
        view = views.GenericSQLView(
            schema=None,
            name=None,
            query=f"""
            SELECT *
            FROM (
                SELECT
                    table_name, column_name, 'REMOVED' AS diff_kind
                FROM (
                    SELECT table_name, column_name
                    FROM {destination_dataset}.INFORMATION_SCHEMA.COLUMNS
                    EXCEPT
                    DISTINCT
                    SELECT table_name, column_name
                    FROM {origin_dataset}.INFORMATION_SCHEMA.COLUMNS
                )

                UNION ALL

                SELECT
                    table_name, NULL AS column_name, 'REMOVED' AS diff_kind
                FROM (
                    SELECT table_name
                    FROM {destination_dataset}.INFORMATION_SCHEMA.TABLES
                    EXCEPT DISTINCT
                    SELECT table_name
                    FROM {origin_dataset}.INFORMATION_SCHEMA.TABLES
                )

                UNION ALL

                SELECT
                    table_name, column_name, 'ADDED' AS diff_kind
                FROM (
                    SELECT table_name, column_name
                    FROM {origin_dataset}.INFORMATION_SCHEMA.COLUMNS
                    EXCEPT DISTINCT
                    SELECT table_name, column_name
                    FROM {destination_dataset}.INFORMATION_SCHEMA.COLUMNS
                )

                UNION ALL

                SELECT
                    table_name, NULL AS column_name, 'ADDED' AS diff_kind
                FROM (
                    SELECT table_name
                    FROM {origin_dataset}.INFORMATION_SCHEMA.TABLES
                    EXCEPT DISTINCT
                    SELECT table_name
                    FROM {destination_dataset}.INFORMATION_SCHEMA.TABLES
                )
            )
            WHERE table_name != 'None__None'
            """,
        )
        return self._load_sql(view)
