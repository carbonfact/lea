from __future__ import annotations

import dataclasses
from lea import scripts
from lea.dialects import BigQueryDialect

from google.cloud import bigquery
import pandas as pd



@dataclasses.dataclass
class JobResult:
    billed_dollars: float
    output_dataframe: pd.DataFrame = None


class BigQueryClient:
    def __init__(
        self,
        credentials,
        location,
        write_project_id,
        compute_project_id,
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

    def materialize_script(self, script: scripts.Script, is_dry_run: bool) -> JobResult:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script, is_dry_run=is_dry_run)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript, is_dry_run: bool) -> JobResult:
        table_ref_str = BigQueryDialect.format_table_ref(sql_script.table_ref)
        job_config = self.make_job_config(
            destination=bigquery.TableReference.from_string(f"{self.write_project_id}.{table_ref_str}"),
            write_disposition="WRITE_TRUNCATE",
            dry_run=is_dry_run
        )
        job = self.client.query(sql_script.code, job_config=job_config)
        job.result()
        bytes_billed = job.total_bytes_processed if is_dry_run else job.total_bytes_billed
        return JobResult(
            billed_dollars=self.estimate_cost_in_dollars(bytes_billed),
        )

    def query_script(self, script: scripts.Script, is_dry_run: bool) -> JobResult:
        if isinstance(script, scripts.SQLScript):
            return self.query_sql_script(sql_script=script, is_dry_run=is_dry_run)
        raise ValueError("Unsupported script type")

    def query_sql_script(self, sql_script: scripts.SQLScript, is_dry_run: bool) -> JobResult:
        job_config = self.make_job_config(dry_run=is_dry_run)
        job = self.client.query(sql_script.code, job_config=job_config)
        job_result = job.result()
        bytes_billed = job.total_bytes_processed if is_dry_run else job.total_bytes_billed
        return JobResult(
            output_dataframe=job_result.to_dataframe(),
            billed_dollars=self.estimate_cost_in_dollars(bytes_billed)
        )

    def clone_table(self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef):
        clone_code = f"""
        CREATE OR REPLACE TABLE
        {BigQueryDialect.format_table_ref(to_table_ref)}
        CLONE {BigQueryDialect.format_table_ref(from_table_ref)}
        """
        job_config = self.make_job_config()
        job = self.client.query(clone_code, job_config=job_config)
        job.result()
        return JobResult(
            billed_dollars=self.estimate_cost_in_dollars(job.total_bytes_billed),
        )

    def make_job_config(self, **kwargs) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.INTERACTIVE,
            use_query_cache=False,
            **kwargs
        )


Client = BigQueryClient
