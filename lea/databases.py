from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import hashlib
import typing
import urllib.parse
from pathlib import Path

import duckdb
import pandas as pd
import requests
import rich
from google.cloud import bigquery
from google.oauth2 import service_account

import lea
from lea import scripts
from lea.dialects import BigQueryDialect, DuckDBDialect
from lea.field import FieldTag
from lea.table_ref import TableRef


class Warehouse(enum.Enum):
    BIGQUERY = "bigquery"
    DUCKDB = "duckdb"
    MOTHERDUCK = "motherduck"
    DUCKLAKE = "ducklake"


class DatabaseJob(typing.Protocol):
    @property
    def is_done(self) -> bool:
        ...

    def stop(self):
        ...

    @property
    def result(self) -> pd.DataFrame:
        ...

    @property
    def exception(self) -> Exception | None:
        ...

    @property
    def billed_dollars(self) -> float | None:
        ...

    @property
    def statistics(self) -> TableStats | None:
        ...

    @property
    def metadata(self) -> list[str]:
        return []

    def conclude(self):
        ...


class DatabaseClient(typing.Protocol):
    def create_dataset(self, dataset_name: str):
        ...

    def delete_dataset(self, dataset_name: str):
        ...

    def materialize_script(self, script: scripts.Script) -> DatabaseJob:
        ...

    def query_script(self, script: scripts.Script) -> DatabaseJob:
        ...

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> DatabaseJob:
        ...

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> DatabaseJob:
        ...

    def delete_table(self, table_ref: scripts.TableRef) -> DatabaseJob:
        ...

    def list_table_stats(self, dataset_name: str) -> dict[scripts.TableRef, TableStats]:
        ...

    def list_table_fields(self, dataset_name: str) -> dict[scripts.TableRef, list[scripts.Field]]:
        ...


@dataclasses.dataclass
class BigQueryJob:
    client: BigQueryClient
    query_job: bigquery.QueryJob
    destination: bigquery.TableReference | None = None
    script: scripts.SQLScript | None = None

    @property
    def is_done(self) -> bool:
        return self.query_job.done()

    @property
    def billed_dollars(self) -> float | None:
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
    def exception(self) -> Exception | None:
        return self.query_job.exception()

    @property
    def is_using_reservation(self) -> bool:
        return (
            self.query_job._properties.get("statistics", {})
            .get("reservationUsage", [{}])[0]
            .get("name")
        ) is not None

    @property
    def metadata(self) -> list[str]:
        billing_model = ("reservation" if self.is_using_reservation else "on-demand") + " billing"
        return [billing_model]

    def conclude(self):
        if self.client.big_blue_pick_api is not None and self.script is not None:
            self.client.big_blue_pick_api.record_job_for_script(
                script=self.script, job=self.query_job
            )


@dataclasses.dataclass(frozen=True)
class TableStats:
    n_rows: int | None
    n_bytes: int | None
    updated_at: dt.datetime | None


class BigBluePickAPI:
    """Big Blue Pick API implementation.

    https://biq.blue/blog/compute/how-to-implement-bigquery-autoscaling-reservation-in-10-minutes

    Parameters
    ----------
    on_demand_project_id
        The project ID of the on-demand BigQuery project.
    reservation_project_id
        The project ID of the reservation BigQuery project.
    default_project_id
        The project ID of the default BigQuery project. This is used if something with the
        BigBlue Pick API fails.

    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        on_demand_project_id: str,
        reservation_project_id: str,
        default_project_id: str,
    ):
        self.api_url = api_url
        self.api_key = api_key
        self.on_demand_project_id = on_demand_project_id
        self.reservation_project_id = reservation_project_id
        self.default_project_id = default_project_id

    def call_pick_api(self, path, body):
        try:
            response = requests.post(
                urllib.parse.urljoin(self.api_url, path),
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            lea.log.warning(f"Big Blue Pick API call failed: {e}")
            return None

    @staticmethod
    def hash_script(script: scripts.SQLScript) -> str:
        return hashlib.sha256(
            str(script.table_ref.replace_dataset("FREEZE").replace_project("FREEZE")).encode()
        ).hexdigest()

    def pick_project_id_for_script(self, script: scripts.SQLScript) -> str:
        response = self.call_pick_api(
            path="/pick",
            body={"hash": self.hash_script(script)},
        )
        if not response or not (pick := response.get("pick")):
            lea.log.warning("Big Blue Pick API call failed, using default project ID")
        elif pick not in {"ON-DEMAND", "RESERVATION"}:
            lea.log.warning(
                f"Big Blue Pick API returned unexpected choice {response['pick']!r}, using default project ID"
            )
        elif pick == "ON-DEMAND":
            return self.on_demand_project_id
        elif pick == "RESERVATION":
            return self.reservation_project_id
        return self.default_project_id

    def pick_client(
        self,
        script: scripts.SQLScript,
        credentials: service_account.Credentials | None,
        location: str,
    ) -> bigquery.Client:
        project_id = self.pick_project_id_for_script(script=script)
        return bigquery.Client(project=project_id, credentials=credentials, location=location)

    def record_job_for_script(self, script: scripts.SQLScript, job: bigquery.QueryJob):
        self.call_pick_api(
            path="/write",
            # https://github.com/biqblue/docs/blob/1ec0eae06ccfabb339cf11bc19dbcbe04b404373/examples/python/pick.py#L42
            body={
                "hash": self.hash_script(script),
                "job_id": job.job_id,
                "creation_time": str(int(job.created.timestamp() * 1000)),
                "start_time": str(int(job.started.timestamp() * 1000)),
                "end_time": str(int(job.ended.timestamp() * 1000)),
                "total_slot_ms": job.slot_millis,
                "total_bytes_billed": job.total_bytes_billed,
                "total_bytes_processed": job.total_bytes_processed,
                "bi_engine_mode": getattr(job, "bi_engine_statistics", {}).get(
                    "bi_engine_mode", ""
                ),
                "reservation_id": (
                    job._properties.get("statistics", {})
                    .get("reservationUsage", [{}])[0]
                    .get("name", "")
                ),
            },
        )


class BigQueryClient(BigBluePickAPI):
    def __init__(
        self,
        credentials: service_account.Credentials | None,
        location: str,
        write_project_id: str,
        compute_project_id: str | None,
        storage_billing_model: str = "PHYSICAL",
        dry_run: bool = False,
        print_mode: bool = False,
        default_clustering_fields: list[str] | None = None,
        big_blue_pick_api_url: str | None = None,
        big_blue_pick_api_key: str | None = None,
        big_blue_pick_api_on_demand_project_id: str | None = None,
        big_blue_pick_api_reservation_project_id: str | None = None,
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
            client_options={
                "scopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/spreadsheets.readonly",
                    "https://www.googleapis.com/auth/userinfo.email",
                ]
            },
        )
        self.dry_run = dry_run
        self.print_mode = print_mode
        self.default_clustering_fields = default_clustering_fields or []

        self.big_blue_pick_api = (
            BigBluePickAPI(
                api_url=big_blue_pick_api_url,
                api_key=big_blue_pick_api_key,
                on_demand_project_id=big_blue_pick_api_on_demand_project_id,
                reservation_project_id=big_blue_pick_api_reservation_project_id,
                default_project_id=self.write_project_id,
            )
            if (
                big_blue_pick_api_url is not None
                and big_blue_pick_api_key is not None
                and big_blue_pick_api_on_demand_project_id is not None
                and big_blue_pick_api_reservation_project_id is not None
            )
            else None
        )

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
        default_clustering_fields = [
            clustering_field
            for clustering_field in (self.default_clustering_fields or [])
            if clustering_field in {field.name for field in sql_script.fields or []}
        ]
        tagged_clustering_fields = [
            field.name
            for field in (sql_script.fields or [])
            if FieldTag.CLUSTERING_FIELD in field.tags
        ]
        clustering_fields = []
        # Remove duplicates but preserve order
        clustering_fields = list(
            dict.fromkeys([*default_clustering_fields, *tagged_clustering_fields])
        )
        job_config = self.make_job_config(
            script=sql_script,
            destination=destination,
            write_disposition="WRITE_TRUNCATE",
            clustering_fields=clustering_fields
            if clustering_fields and not sql_script.table_ref.is_test
            else None,
        )

        client = (
            self.big_blue_pick_api.pick_client(
                script=sql_script,
                credentials=self.credentials,
                location=self.location,
            )
            if self.big_blue_pick_api is not None
            else self.client
        )

        # Run header statements if there are any
        if sql_script.header_statements:
            code = "".join(f"{hs};\n" for hs in sql_script.header_statements)
            header_job_config = bigquery.QueryJobConfig(create_session=True)
            job = client.query(code, job_config=header_job_config)
            job.result()
            session_id = job.session_info.session_id  # type: ignore
        else:
            session_id = None

        if session_id is not None:
            job_config.connection_properties = [
                bigquery.ConnectionProperty(key="session_id", value=session_id)
            ]

        return BigQueryJob(
            client=self,
            query_job=client.query(
                query=sql_script.query,
                job_config=job_config,
                location=self.location,
            ),
            destination=destination,
            script=sql_script,
        )

    def query_script(self, script: scripts.Script) -> BigQueryJob:
        if isinstance(script, scripts.SQLScript):
            return self.query_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def query_sql_script(self, sql_script: scripts.SQLScript) -> BigQueryJob:
        job_config = self.make_job_config(script=sql_script)
        client = (
            self.big_blue_pick_api.pick_client(
                script=sql_script,
                credentials=self.credentials,
                location=self.location,
            )
            if self.big_blue_pick_api is not None
            else self.client
        )
        return BigQueryJob(
            client=self,
            query_job=client.query(
                query=sql_script.query, job_config=job_config, location=self.location
            ),
            script=sql_script,
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

        # First, delete the destination table if it exists. We need to do this because the existing
        # table potentially has a different clustering configuration, which cannot be changed with
        # a CLONE.
        delete_code = f"""
        DROP TABLE IF EXISTS {destination};
        """
        header_job_config = bigquery.QueryJobConfig(create_session=True)
        job = self.client.query(delete_code, job_config=header_job_config)
        job.result()
        session_id = job.session_info.session_id

        # Now, clone the source table to the destination.
        clone_code = f"""
        CREATE TABLE {destination}
        CLONE {source}
        """
        job_config = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref, code=clone_code, sql_dialect=BigQueryDialect, fields=[]
            ),
            connection_properties=[bigquery.ConnectionProperty(key="session_id", value=session_id)],
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

    def list_table_fields(self, dataset_name: str) -> dict[scripts.TableRef, list[scripts.Field]]:
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
        try:
            self.execute()
        except Exception as e:
            self.exception = repr(e)
            raise e
        else:
            return True

    def stop(self):
        pass  # No support for stopping queries in DuckDB

    @property
    def result(self) -> pd.DataFrame:
        return self.connection.execute(self.query).fetchdf()

    @property
    def billed_dollars(self) -> float | None:
        return None  # DuckDB is free to use

    @property
    def statistics(self) -> TableStats | None:
        query = f"SELECT COUNT(*) AS n_rows, MAX(_materialized_timestamp) AS updated_at FROM {self.destination}"
        table = self.connection.execute(query).fetchdf()
        if table.empty:
            return None
        return TableStats(
            n_rows=int(table.iloc[0]["n_rows"]),
            n_bytes=None,
            updated_at=table.iloc[0]["updated_at"],
        )

    @property
    def metadata(self) -> list[str]:
        return []

    def conclude(self):
        pass


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

    def create_schema(self, schema_name: str):
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def materialize_script(self, script: scripts.Script) -> DuckDBJob:
        if isinstance(script, scripts.SQLScript):
            return self.materialize_sql_script(sql_script=script)
        raise ValueError("Unsupported script type")

    def materialize_sql_script(self, sql_script: scripts.SQLScript) -> DuckDBJob:
        if sql_script.header_statements:
            raise ValueError("Header statements are not (yet) supported for DuckDB")

        destination = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=sql_script.table_ref
        )
        # We need to materialize the script with a timestamp to keep track of when it was materialized.
        # DuckDB does not provide a metadata table, so we need to create one with a technical column.
        # bear in mind that this is a workaround and not a best practice. Any change done outside
        # lea will not be reflected in the metadata column and could break orchestration mecanism.
        materialize_code = f"""
        CREATE OR REPLACE TABLE {destination} AS (
        WITH logic_table AS ({sql_script.query}),
        materialized_infos AS (SELECT CURRENT_LOCALTIMESTAMP() AS _materialized_timestamp)
        SELECT * FROM logic_table, materialized_infos
        );
        """
        return self.make_job_config(
            script=scripts.SQLScript(
                table_ref=sql_script.table_ref,
                code=materialize_code,
                sql_dialect=DuckDBDialect,
                fields=[],
            ),
            destination=destination,
        )

    def query_script(self, script: scripts.Script) -> DuckDBJob:
        if isinstance(script, scripts.SQLScript):
            job = self.make_job_config(script=script)
            return job
        raise ValueError("Unsupported script type")

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> DuckDBJob:
        destination = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=to_table_ref
        )
        source = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(table_ref=from_table_ref)
        clone_code = f"""
        CREATE OR REPLACE TABLE {destination} AS SELECT * FROM {source}
        """
        job = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref, code=clone_code, sql_dialect=DuckDBDialect, fields=[]
            ),
            destination=destination,
        )
        return job

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> DuckDBJob:
        to_table_reference = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=to_table_ref
        )
        from_table_reference = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=from_table_ref
        )

        delete_and_insert_code = f"""
        DELETE FROM {to_table_reference} WHERE {on} IN (SELECT DISTINCT {on} FROM {from_table_reference});
        INSERT INTO {to_table_reference} SELECT * FROM {from_table_reference};
        """
        job = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref,
                code=delete_and_insert_code,
                sql_dialect=DuckDBDialect,
                fields=[],
            ),
            destination=to_table_reference,
        )
        job.execute()
        return job

    def delete_table(self, table_ref: scripts.TableRef) -> DuckDBJob:
        table_reference = DuckDBDialect.convert_table_ref_to_duckdb_table_reference(
            table_ref=table_ref
        )
        delete_code = f"DROP TABLE IF EXISTS {table_reference}"
        job = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=table_ref, code=delete_code, sql_dialect=DuckDBDialect, fields=[]
            )
        )
        job.execute()
        return job

    @property
    def _tables_query(self) -> str:
        return """
        SELECT table_name, schema_name, estimated_size
        FROM duckdb_tables();
        """

    def list_table_stats(self, dataset_name: str) -> dict[TableRef, TableStats]:
        tables_result = self.connection.execute(self._tables_query).fetchdf()

        table_stats = {}
        for _, row in tables_result.iterrows():
            table_name = row["table_name"]
            table_schema = row["schema_name"]
            n_rows = int(row["estimated_size"]) if not pd.isna(row["estimated_size"]) else None
            stats_query = f"""
            SELECT
                MAX(_materialized_timestamp) AS last_modified
            FROM {table_schema}.{table_name}
            """
            result = self.connection.execute(stats_query).fetchdf().dropna()
            if result.empty:
                updated_at = dt.datetime.now(dt.timezone.utc)
            else:
                updated_at = dt.datetime.fromtimestamp(
                    result.iloc[0]["last_modified"].to_pydatetime().timestamp(),
                    tz=dt.timezone.utc,
                )
            table_stats[
                DuckDBDialect.parse_table_ref(f"{table_schema}.{table_name}").replace_dataset(
                    dataset_name
                )
            ] = TableStats(
                n_rows=n_rows,
                n_bytes=None,
                updated_at=updated_at,
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
            scripts.TableRef(dataset=None, schema=(), name=str(table_name), project=None): [
                scripts.Field(name=row["column_name"]) for _, row in rows.iterrows()
            ]
            for table_name, rows in result.groupby("table_name")
        }

    def make_job_config(
        self, script: scripts.SQLScript, destination: str | None = None
    ) -> DuckDBJob:
        if self.print_mode:
            rich.print(script)
        job = DuckDBJob(query=script.query, connection=self.connection, destination=destination)
        return job


class MotherDuckClient(DuckDBClient):
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb

    @property
    def _tables_query(self) -> str:
        return f"""
        SELECT table_name, schema_name, estimated_size
        FROM duckdb_tables()
        WHERE database_name = '{self.database_path.stem}';
        """


class DuckLakeClient(DuckDBClient):
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb

    @property
    def _tables_query(self) -> str:
        return """
        SELECT table_name, schema_name, estimated_size
        FROM duckdb_tables()
        WHERE NOT STARTS_WITH(table_name, 'ducklake_');
        """
