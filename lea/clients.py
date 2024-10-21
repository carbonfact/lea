from __future__ import annotations

import dataclasses
from lea import scripts
from lea.dialects import BigQueryDialect

from google.cloud import bigquery
import pandas as pd



@dataclasses.dataclass
class MaterializationResult:
    billed_dollars: float


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

    def materialize_script(self, script: scripts.Script, is_dry_run: bool) -> MaterializationResult:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script, is_dry_run=is_dry_run)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript, is_dry_run: bool) -> MaterializationResult:
        job_config = self.make_job_config(sql_script, is_dry_run=is_dry_run)
        job = self.client.query(sql_script.sql, job_config=job_config)
        job.result()

        cost_per_tb = 5
        bytes_billed = job.total_bytes_processed if is_dry_run else job.total_bytes_billed
        return MaterializationResult(
            billed_dollars=(bytes_billed / 10**12) * cost_per_tb,
        )

    def query_script(self, script: scripts.Script, is_dry_run: bool) -> pd.DataFrame:
        if isinstance(script, scripts.SQLScript):
            return self.query_sql_script(sql_script=script, is_dry_run=is_dry_run)
        raise ValueError("Unsupported script type")

    def query_sql_script(self, sql_script: scripts.SQLScript, is_dry_run: bool) -> pd.DataFrame:
        job_config = self.make_job_config(sql_script, is_dry_run=is_dry_run)
        job = self.client.query(sql_script.sql, job_config=job_config)
        return job.result().to_dataframe()

    def make_job_config(self, script: scripts.SQLScript, is_dry_run: bool) -> bigquery.QueryJobConfig:
        table_ref_str = BigQueryDialect.format_table_ref(script.table_ref)
        return bigquery.QueryJobConfig(
            destination=bigquery.TableReference.from_string(f"{self.write_project_id}.{table_ref_str}"),  # Destination table
            write_disposition="WRITE_TRUNCATE",
            priority=bigquery.QueryPriority.INTERACTIVE,
            use_query_cache=False,
            dry_run=is_dry_run
        )


Client = BigQueryClient
