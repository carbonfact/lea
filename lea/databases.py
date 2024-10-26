from __future__ import annotations

import dataclasses
import typing
from lea import scripts
from lea.dialects import BigQueryDialect

from google.cloud import bigquery
import pandas as pd


class DatabaseJob(typing.Protocol):

    @property
    def is_done(self) -> bool:
        pass

    @property
    def billed_dollars(self) -> float:
        pass

    @property
    def n_rows_in_destination(self) -> int:
        pass

    def cancel(self):
        pass

    @property
    def result(self) -> pd.DataFrame:
        pass


class DatabaseClient(typing.Protocol):

    def create_dataset(self, dataset_name: str):
        pass

    def materialize_script(self, script: scripts.Script) -> DatabaseJob:
        pass

    def query_script(self, script: scripts.Script) -> DatabaseJob:
        pass

    def clone_table(self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef) -> DatabaseJob:
        pass


@dataclasses.dataclass
class BigQueryJob:
    client: BigQueryClient
    query_job: bigquery.QueryJob
    destination: bigquery.TableReference | None = None

    @property
    def is_done(self) -> bool:
        return self.query_job.done()

    @property
    def billed_dollars(self) -> float:
        bytes_billed = self.query_job.total_bytes_processed if self.client.dry_run else self.query_job.total_bytes_billed
        if bytes_billed is None:
            return 0.0
        return self.client.estimate_cost_in_dollars(bytes_billed)

    @property
    def n_rows_in_destination(self) -> int | None:
        if self.client.dry_run:
            return None
        if self.destination is None:
            return None
        return self.client.client.get_table(self.destination).num_rows

    def stop(self):
        self.client.client.cancel_job(self.query_job.job_id)

    @property
    def result(self) -> pd.DataFrame:
        return self.query_job.result().to_dataframe()

    @property
    def exception(self) -> Exception:
        return self.query_job.exception()


class BigQueryClient:
    def __init__(
        self,
        credentials,
        location: str,
        write_project_id: str,
        compute_project_id: str,
        dry_run: bool
    ):
        self.credentials = credentials
        self.write_project_id = write_project_id
        self.compute_project_id = compute_project_id
        self.location = location
        self.client = bigquery.Client(
            project=self.compute_project_id,
            credentials=self.credentials,
            location=self.location,
        )
        self.dry_run = dry_run

    def create_dataset(self, dataset_name: str):
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(
            project=self.write_project_id, dataset_id=dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)

    @staticmethod
    def estimate_cost_in_dollars(bytes_billed: int) -> float:
        cost_per_tb = 5
        return (bytes_billed / 10**12) * cost_per_tb

    def materialize_script(self, script: scripts.Script) -> BigQueryJob:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript) -> BigQueryJob:
        table_ref_str = BigQueryDialect.format_table_ref(sql_script.table_ref)
        destination = bigquery.TableReference.from_string(f"{self.write_project_id}.{table_ref_str}")
        job_config = self.make_job_config(
            destination=destination,
            write_disposition="WRITE_TRUNCATE"
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(sql_script.code, job_config=job_config),
            destination=destination
        )

    def query_script(self, script: scripts.Script) -> BigQueryJob:
        if isinstance(script, scripts.SQLScript):
            return self.query_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def query_sql_script(self, sql_script: scripts.SQLScript) -> BigQueryJob:
        job_config = self.make_job_config(destination=None)
        return BigQueryJob(
            client=self,
            query_job=self.client.query(sql_script.code, job_config=job_config)
        )

    def clone_table(self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef) -> BigQueryJob:
        to_table_ref_str = BigQueryDialect.format_table_ref(to_table_ref)
        destination = bigquery.TableReference.from_string(f"{self.write_project_id}.{to_table_ref_str}")
        clone_code = f"""
        CREATE OR REPLACE TABLE
        {destination}
        CLONE {self.write_project_id}.{BigQueryDialect.format_table_ref(from_table_ref)}
        """
        job_config = self.make_job_config()
        return BigQueryJob(
            client=self,
            query_job=self.client.query(clone_code, job_config=job_config),
            destination=destination
        )

    def delete_and_insert(self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str) -> BigQueryJob:
        from_table_ref_str = BigQueryDialect.format_table_ref(from_table_ref)
        to_table_ref_str = BigQueryDialect.format_table_ref(to_table_ref)
        delete_and_insert_code = f"""
        BEGIN TRANSACTION;

        -- Delete existing data
        DELETE FROM {to_table_ref_str}
        WHERE {on} IN (SELECT DISTINCT {on} FROM {from_table_ref_str});

        -- Insert new data
        INSERT INTO {to_table_ref_str}
        SELECT * FROM {from_table_ref_str};

        COMMIT TRANSACTION;
        """
        job_config = self.make_job_config()
        return BigQueryJob(
            client=self,
            query_job=self.client.query(delete_and_insert_code,  job_config=job_config),
            destination=bigquery.TableReference.from_string(f"{self.write_project_id}.{to_table_ref_str}")
        )

    def make_job_config(self, **kwargs) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.INTERACTIVE,
            use_query_cache=False,
            dry_run=self.dry_run,
            **kwargs
        )
