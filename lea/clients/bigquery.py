from __future__ import annotations

import concurrent.futures
import os

import pandas as pd
import pandas_gbq
import rich.console
import sqlglot

import lea

from .base import Client, QueryResult

# HACK
console = rich.console.Console()


class BigQuery(Client):
    def __init__(
        self,
        credentials,
        location,
        write_project_id,
        compute_project_id,
        dataset_name,
        username,
        wap_mode,
    ):
        self.credentials = credentials
        self.write_project_id = write_project_id
        self.compute_project_id = compute_project_id
        self.location = location
        self._dataset_name = dataset_name
        self.username = username
        self.wap_mode = wap_mode

    def __repr__(self):
        return (
            "Running on BigQuery\n"
            f"{self.dataset_name=}\n"
            f"{self.location=}\n"
            f"{self.write_project_id=}\n"
            f"{self.compute_project_id=}"
        ).replace("self.", "")

    @property
    def dataset_name(self):
        return f"{self._dataset_name}_{self.username}" if self.username else self._dataset_name

    @property
    def sqlglot_dialect(self):
        return sqlglot.dialects.Dialects.BIGQUERY

    @property
    def client(self):
        from google.cloud import bigquery

        return bigquery.Client(
            project=self.compute_project_id,
            credentials=self.credentials,
            location=self.location,
        )

    def prepare(self, views):
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(
            project=self.write_project_id, dataset_id=self.dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        console.log(f"Created dataset {dataset.dataset_id} in project {dataset.project}")

    def teardown(self):
        from google.cloud import bigquery

        dataset_ref = self.client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
        console.log(f"Deleted dataset {dataset.dataset_id}")

    def make_job_config(self, view, labels: dict) -> dict:
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        project_id, schema, table_reference_without_schema = table_reference.split(".", 2)
        return {
            "query": {
                "query": view.query,
                "destinationTable": {
                    "projectId": self.write_project_id,
                    "datasetId": self.dataset_name,
                    "tableId": table_reference_without_schema,
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            },
            "labels": labels
            if labels
            else {
                "job_dataset": self.dataset_name,
                "job_schema": schema,
                "job_table": table_reference_without_schema.replace(
                    f"{lea._SEP}{lea._WAP_MODE_SUFFIX}", ""
                ),
                "job_username": self.username,
                "job_is_github_actions": "GITHUB_ACTIONS" in os.environ,
                "service": "lea",
            },
            "location": self.location,
        }

    def materialize_sql_view(self, view) -> QueryResult:
        job_config = self.make_job_config(view, labels={})
        job = self.client.create_job(job_config)
        job.result()
        cost_per_tb = 5
        return QueryResult(cost=(job.total_bytes_processed / 10**12) * cost_per_tb)

    def materialize_python_view(self, view):
        dataframe = self.read_python_view(view)
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        self._materialize_pandas_dataframe(dataframe, table_reference)

    def materialize_json_view(self, view):
        dataframe = pd.read_json(view.path)
        table_reference = self._view_key_to_table_reference(view.key, with_context=True)
        self._materialize_pandas_dataframe(dataframe, table_reference)

    def _materialize_pandas_dataframe(self, dataframe, table_reference):
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )
        job = self.client.load_table_from_dataframe(
            dataframe,
            table_reference.replace("`", ""),
            job_config=job_config,
        )
        job.result()

    def delete_table_reference(self, table_reference):
        _, table_reference = table_reference.rsplit(".", 1)
        self.client.delete_table(f"{self.write_project_id}.{self.dataset_name}.{table_reference}")

    def read_sql(self, query: str) -> pd.DataFrame:
        return pandas_gbq.read_gbq(
            query,
            credentials=self.client._credentials,
            project_id=self.write_project_id,
            location=self.location,
            progress_bar_type=None,
        )

    def list_tables(self):
        return self.read_sql(
            f"""
        SELECT
            FORMAT('%s.%s', '{self.dataset_name}', table_id) AS table_reference,
            row_count AS n_rows,
            size_bytes AS n_bytes
        FROM `{self.write_project_id}.{self.dataset_name}`.__TABLES__
        """
        )

    def list_columns(self) -> pd.DataFrame:
        return self.read_sql(
            f"""
        SELECT
            FORMAT('%s.%s', table_schema, table_name) AS table_reference,
            column_name AS column,
            data_type AS type
        FROM `{self.write_project_id}.{self.dataset_name}`.INFORMATION_SCHEMA.COLUMNS
        """
        )

    def _view_key_to_table_reference(
        self, view_key: tuple[str], with_context: bool, with_project_id=False
    ) -> str:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     compute_project_id="compute",
        ...     write_project_id="write",
        ...     dataset_name="dataset",
        ...     username="max",
        ...     wap_mode=False
        ... )

        >>> client._view_key_to_table_reference(("schema", "table"), with_context=False)
        'dataset.schema__table'

        >>> client._view_key_to_table_reference(("schema", "subschema", "table"), with_context=False)
        'dataset.schema__subschema__table'

        >>> client._view_key_to_table_reference(("schema", "table"), with_context=False)
        'dataset.schema__table'

        >>> client._view_key_to_table_reference(("schema", "table"), with_context=True)
        '`write`.dataset_max.schema__table'

        """
        table_reference = f"{self._dataset_name}.{lea._SEP.join(view_key)}"
        if with_context:
            table_reference = table_reference.replace(
                f"{self._dataset_name}.", f"{self.dataset_name}."
            )
            table_reference = f"`{self.write_project_id}`.{table_reference}"
            if self.wap_mode:
                table_reference = f"{table_reference}{lea._SEP}{lea._WAP_MODE_SUFFIX}"
        elif with_project_id:
            table_reference = f"{self.write_project_id}.{table_reference}"
        return table_reference

    def _table_reference_to_view_key(self, table_reference: str) -> tuple[str, ...]:
        """

        >>> client = BigQuery(
        ...     credentials=None,
        ...     location="US",
        ...     compute_project_id="compute",
        ...     write_project_id="write",
        ...     dataset_name="dataset",
        ...     username="max",
        ...     wap_mode=False
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
        key = tuple(leftover.split(lea._SEP))
        if dataset not in {self._dataset_name, self.dataset_name}:
            key = (dataset, *key)
        if key[-1] == lea._WAP_MODE_SUFFIX:
            key = key[:-1]
        return key

    def switch_for_wap_mode(self, view_keys):
        def switch(view_key):
            table_reference = self._view_key_to_table_reference(view_key, with_context=True)
            table_reference_without_wap = table_reference.replace(
                f"{lea._SEP}{lea._WAP_MODE_SUFFIX}", ""
            )
            self.client.query(f"DROP TABLE IF EXISTS {table_reference_without_wap}").result()
            self.client.query(
                f"ALTER TABLE {table_reference} RENAME TO {table_reference_without_wap.split('.', 1)[1]}"
            ).result()

        import time

        t = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(view_keys)) as executor:
            jobs = {executor.submit(switch, view_key): view_key for view_key in view_keys}
            for job in concurrent.futures.as_completed(jobs):
                job.result()
        print(f"Switched {len(view_keys)} tables in {time.time() - t:.2f} seconds")

        # HACK: the following doesn't work, so we process the statements sequentially
        # statements = []
        # for view_key in view_keys:
        #     table_reference = self._view_key_to_table_reference(view_key, with_context=True)
        #     table_reference_without_wap = table_reference.replace(f"{lea._SEP}{lea._WAP_MODE_SUFFIX}", "")
        #     statements.append(f"DROP TABLE IF EXISTS {table_reference_without_wap}")
        #     statements.append(
        #         f"ALTER TABLE {table_reference} RENAME TO {table_reference_without_wap.split('.', 1)[1]}"
        #     )
        # try:
        #     # Concatenate all the statements into one string and execute them
        #     # sql = "\n".join(f"{statement};" for statement in statements)
        #     # q = self.client.query(f"BEGIN TRANSACTION; {sql} COMMIT TRANSACTION;")
        #     # q.result()
        #     for statement in statements:
        #         self.client.query(statement).result()
        # except Exception as e:
        #     # Make sure to rollback if there's an error
        #     self.client.query("ROLLBACK TRANSACTION;")
        #     raise e
