from __future__ import annotations

import dataclasses
import datetime as dt
import typing

import pandas as pd
import rich
from google.cloud import bigquery

from lea import scripts
from lea.dialects import BigQueryDialect


class DatabaseJob(typing.Protocol):
    @property
    def is_done(self) -> bool:
        pass

    def stop(self):
        pass

    @property
    def result(self) -> pd.DataFrame:
        pass

    @property
    def exception(self) -> Exception:
        pass

    @property
    def billed_dollars(self) -> float:
        pass

    @property
    def statistics(self) -> TableStats | None:
        pass


class DatabaseClient(typing.Protocol):
    def create_dataset(self, dataset_name: str):
        pass

    def delete_dataset(self, dataset_name: str):
        pass

    def materialize_script(self, script: scripts.Script) -> DatabaseJob:
        pass

    def query_script(self, script: scripts.Script) -> DatabaseJob:
        pass

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> DatabaseJob:
        pass

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> DatabaseJob:
        pass

    def delete_table(self, table_ref: scripts.TableRef) -> DatabaseJob:
        pass

    def list_table_stats(self, dataset_name: str) -> dict[scripts.TableRef, TableStats]:
        pass

    def list_table_fields(self, dataset_name: str) -> dict[scripts.TableRef, list[scripts.Field]]:
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
        bytes_billed = (
            self.query_job.total_bytes_processed
            if self.client.dry_run
            else self.query_job.total_bytes_billed
        )
        if bytes_billed is None:
            return 0.0
        return self.client.estimate_cost_in_dollars(bytes_billed)

    @property
    def statistics(self) -> TableStats | None:
        if self.client.dry_run or self.destination is None:
            return None
        table = self.client.client.get_table(
            self.destination, retry=bigquery.DEFAULT_RETRY.with_deadline(10)
        )
        return TableStats(n_rows=table.num_rows, n_bytes=table.num_bytes, updated_at=table.modified)

    def stop(self):
        self.client.client.cancel_job(self.query_job.job_id)

    @property
    def result(self) -> pd.DataFrame:
        return self.query_job.result().to_dataframe()

    @property
    def exception(self) -> Exception:
        return self.query_job.exception()


@dataclasses.dataclass(frozen=True)
class TableStats:
    n_rows: int
    n_bytes: int
    updated_at: dt.datetime


class BigQueryClient:
    def __init__(
        self,
        credentials,
        location: str,
        write_project_id: str,
        compute_project_id: str,
        dry_run: bool = False,
        print_mode: bool = False,
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
        self.print_mode = print_mode

    def create_dataset(self, dataset_name: str):
        dataset_ref = bigquery.DatasetReference(
            project=self.write_project_id, dataset_id=dataset_name
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=True)

    def delete_dataset(self, dataset_name: str):
        self.client.delete_dataset(
            dataset=f"{self.write_project_id}.{dataset_name}",
            delete_contents=True,
            not_found_ok=True,
        )

    @staticmethod
    def estimate_cost_in_dollars(bytes_billed: int) -> float:
        cost_per_tb = 5
        return (bytes_billed / 10**12) * cost_per_tb

    def materialize_script(self, script: scripts.Script) -> BigQueryJob:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript) -> BigQueryJob:
        destination = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=sql_script.table_ref, project=self.write_project_id
        )
        job_config = self.make_job_config(
            script=sql_script, destination=destination, write_disposition="WRITE_TRUNCATE"
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(
                query=sql_script.code, job_config=job_config, location=self.location
            ),
            destination=destination,
        )

    def query_script(self, script: scripts.Script) -> BigQueryJob:
        if isinstance(script, scripts.SQLScript):
            return self.query_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def query_sql_script(self, sql_script: scripts.SQLScript) -> BigQueryJob:
        job_config = self.make_job_config(script=sql_script)
        return BigQueryJob(
            client=self,
            query_job=self.client.query(
                query=sql_script.code, job_config=job_config, location=self.location
            ),
        )

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> BigQueryJob:
        destination = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=to_table_ref, project=self.write_project_id
        )
        source = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=from_table_ref, project=self.write_project_id
        )
        clone_code = f"""
        CREATE OR REPLACE TABLE {destination}
        CLONE {source}
        """
        job_config = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref, code=clone_code, sql_dialect=BigQueryDialect, fields=[]
            )
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(clone_code, job_config=job_config, location=self.location),
            destination=destination,
        )

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> BigQueryJob:
        source = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=from_table_ref, project=self.write_project_id
        )
        destination = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=to_table_ref, project=self.write_project_id
        )
        # TODO: the following could instead be done with a MERGE statement.
        delete_and_insert_code = f"""
        BEGIN TRANSACTION;

        -- Delete existing data
        DELETE FROM {destination}
        WHERE {on} IN (SELECT DISTINCT {on} FROM {source});

        -- Insert new data
        INSERT INTO {destination}
        SELECT * FROM {source};

        COMMIT TRANSACTION;
        """
        job_config = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref,
                code=delete_and_insert_code,
                sql_dialect=BigQueryDialect,
                fields=[],
            )
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(
                delete_and_insert_code, job_config=job_config, location=self.location
            ),
            destination=destination,
        )

    def delete_table(self, table_ref: scripts.TableRef) -> BigQueryJob:
        table_reference = BigQueryDialect.convert_table_ref_to_bigquery_table_reference(
            table_ref=table_ref, project=self.write_project_id
        )
        delete_code = f"""
        DROP TABLE IF EXISTS {table_reference}
        """
        job_config = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=table_ref,
                code=delete_code,
                sql_dialect=BigQueryDialect,
                fields=[],
            )
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(delete_code, job_config=job_config, location=self.location),
        )

    def list_table_stats(self, dataset_name: str) -> dict[scripts.TableRef, TableStats]:
        query = f"""
        SELECT table_id, row_count, size_bytes, last_modified_time
        FROM `{self.write_project_id}.{dataset_name}.__TABLES__`
        """
        job = self.client.query(query, location=self.location)
        return {
            BigQueryDialect.parse_table_ref(
                f"{self.write_project_id}.{dataset_name}.{row['table_id']}"
            ): TableStats(
                n_rows=row["row_count"],
                n_bytes=row["size_bytes"],
                updated_at=(
                    dt.datetime.fromtimestamp(row["last_modified_time"] // 1000, tz=dt.timezone.utc)
                ),
            )
            for row in job.result()
        }

    def list_table_fields(self, dataset_name: str) -> dict[scripts.TableRef, set[scripts.Field]]:
        query = f"""
        SELECT table_name, column_name
        FROM `{self.write_project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
        """
        job = self.client.query(query, location=self.location)
        return {
            BigQueryDialect.parse_table_ref(
                f"{self.write_project_id}.{dataset_name}.{table_name}"
            ): [scripts.Field(name=row["column_name"]) for _, row in rows.iterrows()]
            for table_name, rows in job.result()
            .to_dataframe()
            .sort_values(["table_name", "column_name"])
            .groupby("table_name")
        }

    def make_job_config(self, script: scripts.SQLScript, **kwargs) -> bigquery.QueryJobConfig:
        if self.print_mode:
            rich.print(script)
        return bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.INTERACTIVE,
            use_query_cache=False,
            dry_run=self.dry_run,
            **kwargs,
        )

        # The approach we use works best if the tables are clustered by account_slug. This is
        # because we only need to refresh the data for a subset of accounts, and clustering by
        # account_slug allows BigQuery to only scan the data for the accounts that need to be
        # refreshed. This is a good practice in general, but it's particularly important in this
        # case.
        # WIP
        # if (
        #     script is not None
        #     and not script.table_ref.is_test
        #     and script.table_ref.name.endswith("___audit")
        #     and "account_slug" in {field.name for field in script.fields}
        # ):
        #     job_config.clustering_fields = ["account_slug"]

        # return job_config
