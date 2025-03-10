from __future__ import annotations

import dataclasses
import datetime as dt
import typing
from pathlib import Path

import duckdb
import pandas as pd
import rich
from google.cloud import bigquery

from lea import scripts
from lea.dialects import BigQueryDialect, DuckDBDialect
from lea.table_ref import TableRef


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
    n_bytes: int | None
    updated_at: dt.datetime


class BigQueryClient:
    def __init__(
        self,
        credentials,
        location: str,
        write_project_id: str,
        compute_project_id: str,
        storage_billing_model: str = "PHYSICAL",
        dry_run: bool = False,
        print_mode: bool = False,
    ):
        self.credentials = credentials
        self.write_project_id = write_project_id
        self.compute_project_id = compute_project_id
        self.storage_billing_model = storage_billing_model
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
        dataset.storage_billing_model = self.storage_billing_model
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


@dataclasses.dataclass
class DuckDBJob:
    query: str
    connection: duckdb.DuckDBPyConnection
    destination: str | None = None
    exception: str | None = None

    def execute(self):
        self.connection.execute(self.query)

    @property
    def is_done(self) -> bool:
        # try:
        self.execute()
        # except Exception as e:
        #     self.exception = repr(e)
        return True

    def stop(self):
        pass  # No support for stopping queries in DuckDB

    @property
    def result(self) -> pd.DataFrame:
        return self.connection.execute(self.query).fetchdf()

    @property
    def billed_dollars(self) -> float:
        return None  # DuckDB is free to use

    @property
    def statistics(self) -> TableStats | None:
        query = f"SELECT COUNT(*) AS n_rows, MAX(_materialized_timestamp) AS updated_at FROM {self.destination}"
        table = self.connection.execute(query).fetchdf().iloc[0]
        return TableStats(
            n_rows=int(table["n_rows"]),
            n_bytes=None,
            updated_at=table["updated_at"],
        )


class DuckDBClient:
    def __init__(self, database_path: Path, dry_run: bool = False, print_mode: bool = False):
        self.database_path = database_path
        if self.database_path == "":
            raise ValueError("DuckDB path not configured")
        self.dry_run = dry_run
        self.print_mode = print_mode

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(database=str(self.database_path))

    @property
    def dataset(self) -> str:
        return self.database_path.stem

    def create_dataset(self, dataset_name: str):
        self.database_path = self.database_path.with_stem(dataset_name)

    def create_schema(self, table_ref: scripts.TableRef):
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {table_ref.schema[0]}")

    def materialize_script(self, script: scripts.Script) -> DuckDBJob:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript) -> DuckDBJob:
        destination = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=sql_script.table_ref
        )
        # add a technical field to add current timestamp to the table
        materialize_code = f"""
        CREATE OR REPLACE TABLE {destination} AS (
        WITH logic_table AS ({sql_script.code}),
        materialized_infos AS (SELECT CURRENT_LOCALTIMESTAMP() AS _materialized_timestamp)
        SELECT * FROM logic_table, materialized_infos
        );
        """

        job = DuckDBJob(query=materialize_code, connection=self.connection, destination=destination)
        return job

    def query_script(self, script: scripts.Script) -> DuckDBJob:
        if isinstance(script, scripts.SQLScript):
            return DuckDBJob(query=script.code, connection=self.connection)
        raise ValueError("Unsupported script type")

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> DuckDBJob:
        destination = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=to_table_ref
        )
        source = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(table_ref=from_table_ref)
        clone_code = f"""
        DROP TABLE IF EXISTS {destination};
        CREATE TABLE {destination} AS SELECT * FROM {source}
        """
        return DuckDBJob(query=clone_code, connection=self.connection, destination=destination)

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> DuckDBJob:
        delete_and_insert_code = f"""
        DELETE FROM {to_table_ref} WHERE {on} IN (SELECT DISTINCT {on} FROM {from_table_ref});
        INSERT INTO {to_table_ref} SELECT * FROM {from_table_ref};
        """
        return DuckDBJob(query=delete_and_insert_code, connection=self.connection)

    def delete_table(self, table_ref: scripts.TableRef) -> DuckDBJob:
        table_reference = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=table_ref
        )
        delete_code = f"DROP TABLE IF EXISTS {table_reference}"
        job = DuckDBJob(query=delete_code, connection=self.connection)
        job.execute()
        return job

    def list_table_stats(self, dataset_name: str) -> dict[TableRef, TableStats]:
        tables_query = """
        SELECT table_name, table_schema
        FROM information_schema.tables
        """
        tables_result = self.connection.execute(tables_query).fetchdf()

        table_stats = {}
        for _, row in tables_result.iterrows():
            table_name = row["table_name"]
            table_schema = row["table_schema"]
            stats_query = f"""
            SELECT
                COUNT(*) AS n_rows,
                MAX(_materialized_timestamp) AS last_modified
            FROM {table_schema}.{table_name}
            """
            stats_result = self.connection.execute(stats_query).fetchdf().iloc[0]
            table_stats[
                DuckDBDialect.parse_table_ref(f"{table_schema}.{table_name}").replace_dataset(
                    dataset_name
                )
            ] = TableStats(
                n_rows=int(stats_result["n_rows"]),
                n_bytes=None,
                updated_at=(
                    dt.datetime.fromtimestamp(
                        stats_result["last_modified"].to_pydatetime().timestamp(),
                        tz=dt.timezone.utc,
                    )
                ),
            )
        return table_stats

    def list_table_fields(self, dataset_name: str) -> dict[scripts.TableRef, list[scripts.Field]]:
        query = f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = '{dataset_name}'
        """
        result = self.connection.execute(query).fetchdf()
        return {
            scripts.TableRef(name=table_name): [
                scripts.Field(name=row["column_name"]) for _, row in rows.iterrows()
            ]
            for table_name, rows in result.groupby("table_name")
        }
