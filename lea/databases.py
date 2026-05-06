from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import enum
import hashlib
import typing
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import requests
import requests.adapters
import rich

if TYPE_CHECKING:
    import pandas as pd
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

    @property
    def display_name(self) -> str:
        return {
            Warehouse.BIGQUERY: "BigQuery",
            Warehouse.DUCKDB: "DuckDB",
            Warehouse.MOTHERDUCK: "MotherDuck",
            Warehouse.DUCKLAKE: "DuckLake",
        }[self]

    @property
    def rich_name(self) -> str:
        """Display name wrapped in rich markup color tags."""
        color = {
            Warehouse.BIGQUERY: "blue",
            Warehouse.DUCKDB: "green",
            Warehouse.MOTHERDUCK: "magenta",
            Warehouse.DUCKLAKE: "dark_orange",
        }[self]
        return f"[{color}]{self.display_name}[/{color}]"


class DatabaseJob(typing.Protocol):
    @property
    def is_done(self) -> bool: ...

    def stop(self): ...

    @property
    def result(self) -> pd.DataFrame: ...

    @property
    def exception(self) -> Exception | None: ...

    @property
    def billed_dollars(self) -> float | None: ...

    @property
    def statistics(self) -> TableStats | None: ...

    @property
    def metadata(self) -> list[str]:
        return []

    def conclude(self): ...


class DatabaseClient(typing.Protocol):
    def create_dataset(self, dataset_name: str): ...

    def delete_dataset(self, dataset_name: str): ...

    def materialize_script(self, script: scripts.Script) -> DatabaseJob: ...

    def query_script(self, script: scripts.Script) -> DatabaseJob: ...

    def clone_table(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef
    ) -> DatabaseJob: ...

    def delete_and_insert(
        self, from_table_ref: scripts.TableRef, to_table_ref: scripts.TableRef, on: str
    ) -> DatabaseJob: ...

    def delete_table(self, table_ref: scripts.TableRef) -> DatabaseJob: ...

    def list_table_stats(self, dataset_name: str) -> dict[scripts.TableRef, TableStats]: ...

    def list_table_fields(
        self, dataset_name: str
    ) -> dict[scripts.TableRef, list[scripts.Field]]: ...


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
        from google.cloud import bigquery

        if self.client.dry_run or self.destination is None:
            return None
        table = self.client.client.get_table(
            self.destination, retry=bigquery.DEFAULT_RETRY.with_deadline(10)
        )
        return TableStats(n_rows=table.num_rows, n_bytes=table.num_bytes, updated_at=table.modified)

    def stop(self):
        if self.query_job.job_id is not None:
            self.client.client.cancel_job(self.query_job.job_id)

    @property
    def result(self) -> pd.DataFrame:
        return self.query_job.result().to_dataframe()

    @property
    def exception(self) -> Exception | None:
        return self.query_job.exception()

    @property
    def reservation_id(self) -> str | None:
        """The reservation the job actually ran under, or ``None`` for on-demand.

        BigQuery exposes this at ``statistics.reservation_id`` (e.g.
        ``"int-data-kaya-prod:EU.default"``). The older ``reservationUsage`` field only
        fires for slot-consuming queries and is unreliable — don't read from it.
        """
        return self.query_job._properties.get("statistics", {}).get("reservation_id")

    @property
    def metadata(self) -> list[str]:
        billing_model = (
            f"reservation billing ({self.reservation_id})"
            if self.reservation_id is not None
            else "on-demand billing"
        )
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


ON_DEMAND_RESERVATION = "none"


class BigBluePickAPI:
    """Big Blue Pick API implementation.

    https://biq.blue/blog/compute/how-to-implement-bigquery-autoscaling-reservation-in-10-minutes

    Given a script, the Pick API returns either ``"ON-DEMAND"`` or ``"RESERVATION"``. We translate
    those to a value for the BigQuery ``@@reservation`` system variable: ``"none"`` for on-demand,
    and the caller-supplied reservation path for the reservation case.
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        reservation: str,
    ):
        self.api_url = api_url
        self.api_key = api_key
        self.reservation = reservation

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

    def pick_reservation_for_script(self, script: scripts.SQLScript) -> str | None:
        """Return the reservation identifier to use for this script, or None to fall back."""
        response = self.call_pick_api(
            path="/pick",
            body={"hash": self.hash_script(script)},
        )
        if not response or not (pick := response.get("pick")):
            lea.log.warning("Big Blue Pick API call failed, falling back to default reservation")
            return None
        if pick == "ON-DEMAND":
            return ON_DEMAND_RESERVATION
        if pick == "RESERVATION":
            return self.reservation
        lea.log.warning(
            f"Big Blue Pick API returned unexpected choice {pick!r}, falling back to default reservation"
        )
        return None

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


class BigQueryClient:
    def __init__(
        self,
        credentials: service_account.Credentials | None,
        location: str,
        write_project_id: str,
        compute_project_id: str,
        script_specific_reservations: dict[scripts.TableRef, str] | None = None,
        storage_billing_model: str = "PHYSICAL",
        dry_run: bool = False,
        print_mode: bool = False,
        default_clustering_fields: list[str] | None = None,
        big_blue_pick_api_url: str | None = None,
        big_blue_pick_api_key: str | None = None,
        big_blue_pick_api_reservation: str | None = None,
    ):

        self.credentials = credentials
        self.write_project_id = write_project_id
        self.compute_project_id = compute_project_id
        self.script_specific_reservations = script_specific_reservations or {}
        self.storage_billing_model = storage_billing_model
        self.location = location
        self.client = self._make_client(compute_project_id)
        self.dry_run = dry_run
        self.print_mode = print_mode
        self.default_clustering_fields = default_clustering_fields or []

        self.big_blue_pick_api = (
            BigBluePickAPI(
                api_url=big_blue_pick_api_url,
                api_key=big_blue_pick_api_key,
                reservation=big_blue_pick_api_reservation,
            )
            if (
                big_blue_pick_api_url is not None
                and big_blue_pick_api_key is not None
                and big_blue_pick_api_reservation is not None
            )
            else None
        )

    def _make_client(self, project_id: str) -> bigquery.Client:
        from google.cloud import bigquery

        client = bigquery.Client(
            project=project_id,
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
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        client._http.mount("https://", adapter)
        return client

    def create_dataset(self, dataset_name: str):
        from google.cloud import bigquery

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

    def determine_reservation_for_script(self, sql_script: scripts.SQLScript) -> str | None:
        """Resolve which reservation to set via ``SET @@reservation`` for this script.

        Precedence: script-specific override → Big Blue Pick API → ``None`` (meaning: emit no
        SET and let the compute project's GCP-level reservation assignment apply as-is). Static
        overrides win over Pick API so that deliberately routed queries don't get re-routed
        behind the user's back.
        """
        if sql_script.table_ref in self.script_specific_reservations:
            return self.script_specific_reservations[sql_script.table_ref]
        if self.big_blue_pick_api is not None:
            return self.big_blue_pick_api.pick_reservation_for_script(script=sql_script)
        return None

    def _open_session(
        self, reservation: str | None, extra_header_statements: list[str] | None = None
    ) -> str | None:
        """Open a BigQuery session, preloading ``SET @@reservation`` and any header statements.

        Returns the session id, or ``None`` if nothing had to be preloaded (no reservation
        override and no header statements) — in which case the caller should just run its query
        without a session. Skipped entirely when ``dry_run`` is true, since dry-run doesn't
        execute sessions.
        """
        from google.cloud import bigquery

        if self.dry_run:
            return None
        preload: list[str] = []
        if reservation is not None:
            preload.append(f"SET @@reservation = '{reservation}'")
        if extra_header_statements:
            preload.extend(extra_header_statements)
        if not preload:
            return None
        code = "".join(f"{stmt};\n" for stmt in preload)
        header_job_config = bigquery.QueryJobConfig(create_session=True)
        job = self.client.query(code, job_config=header_job_config)
        job.result()
        if job.session_info is None or job.session_info.session_id is None:
            raise RuntimeError(
                "BigQuery did not return a session id after `create_session=True`; cannot "
                "propagate SET @@reservation or header statements to the main query"
            )
        return job.session_info.session_id

    @staticmethod
    def _attach_session(job_config: bigquery.QueryJobConfig, session_id: str | None):
        if session_id is None:
            return
        from google.cloud import bigquery

        job_config.connection_properties = [
            bigquery.ConnectionProperty(key="session_id", value=session_id)
        ]

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
        # Remove duplicates but preserve order
        clustering_fields = list(
            dict.fromkeys([*default_clustering_fields, *tagged_clustering_fields])
        )
        job_config = self.make_job_config(
            script=sql_script,
            destination=destination,
            write_disposition="WRITE_TRUNCATE",
            clustering_fields=(
                clustering_fields
                if clustering_fields and not sql_script.table_ref.is_test
                else None
            ),
        )

        reservation = self.determine_reservation_for_script(sql_script=sql_script)
        if reservation is not None:
            lea.log.info(
                f"Using reservation {reservation!r} for materializing {sql_script.table_ref}"
            )
        session_id = self._open_session(
            reservation=reservation,
            extra_header_statements=list(sql_script.header_statements) or None,
        )
        self._attach_session(job_config, session_id)

        return BigQueryJob(
            client=self,
            query_job=self.client.query(
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
        reservation = self.determine_reservation_for_script(sql_script=sql_script)
        session_id = self._open_session(reservation=reservation)
        self._attach_session(job_config, session_id)
        return BigQueryJob(
            client=self,
            query_job=self.client.query(
                query=sql_script.query,
                job_config=job_config,
                location=self.location,
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
        # a CLONE. Run in a session so the CLONE that follows sees the DROP even if BQ takes a
        # moment to propagate it.
        delete_code = f"DROP TABLE IF EXISTS {destination}"
        session_id = self._open_session(reservation=None, extra_header_statements=[delete_code])

        # Now, clone the source table to the destination.
        clone_code = f"""
        CREATE TABLE {destination}
        CLONE {source}
        """
        job_config = self.make_job_config(
            script=scripts.SQLScript(
                table_ref=to_table_ref, code=clone_code, sql_dialect=BigQueryDialect(), fields=[]
            ),
        )
        self._attach_session(job_config, session_id)

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
                sql_dialect=BigQueryDialect(),
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
                sql_dialect=BigQueryDialect(),
                fields=[],
            )
        )
        return BigQueryJob(
            client=self,
            query_job=self.client.query(
                delete_code, job_config=job_config, location=self.location
            ),
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
                    dt.datetime.fromtimestamp(row["last_modified_time"] // 1000, tz=dt.UTC)
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
        from google.cloud import bigquery

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
    exception: Exception | None = None
    _future: concurrent.futures.Future | None = dataclasses.field(default=None, repr=False)

    def execute(self):
        self.connection.execute(self.query)

    def execute_async(self):
        """Start execution in a background thread so is_done can report progress."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._future = executor.submit(self.execute)
        executor.shutdown(wait=False)

    @property
    def is_done(self) -> bool:
        if self._future is None:
            self.execute_async()
        assert self._future is not None
        if not self._future.done():
            return False
        if exception := self._future.exception():
            self.exception = Exception(exception)
            raise exception
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

    def delete_dataset(self, dataset_name: str):
        pass  # DuckDB does not support deleting datasets

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
                sql_dialect=DuckDBDialect(),
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
                table_ref=to_table_ref, code=clone_code, sql_dialect=DuckDBDialect(), fields=[]
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
                sql_dialect=DuckDBDialect(),
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
                table_ref=table_ref, code=delete_code, sql_dialect=DuckDBDialect(), fields=[]
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
        import pandas as pd

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
                updated_at = dt.datetime.now(dt.UTC)
            else:
                updated_at = dt.datetime.fromtimestamp(
                    result.iloc[0]["last_modified"].to_pydatetime().timestamp(),
                    tz=dt.UTC,
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn = duckdb.connect()
        self._active_database: str | None = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def set_active_database(self, database_name: str):
        self._active_database = database_name

    def make_job_config(
        self, script: scripts.SQLScript, destination: str | None = None
    ) -> DuckDBJob:
        if self.print_mode:
            rich.print(script)
        cursor = self._conn.cursor()
        if self._active_database:
            cursor.execute(f"USE {self._active_database};")
        return DuckDBJob(query=script.query, connection=cursor, destination=destination)

    @property
    def _tables_query(self) -> str:
        return f"""
        SELECT table_name, schema_name, estimated_size
        FROM duckdb_tables()
        WHERE database_name = '{self.database_path.stem}';
        """


class DuckLakeClient(DuckDBClient):
    def __init__(self, *args, catalog_path: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        # Store a persistent connection so that DuckLake attach, extensions, etc. persist
        self._conn = duckdb.connect()
        self._active_database: str | None = None
        self._catalog_path: str | None = catalog_path

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def set_active_database(self, database_name: str):
        """Remember the active database so cursors can inherit it."""
        self._active_database = database_name

    def make_job_config(
        self, script: scripts.SQLScript, destination: str | None = None
    ) -> DuckDBJob:
        if self.print_mode:
            rich.print(script)
        # Use a cursor for thread safety — each job gets its own cursor
        # sharing the parent connection's state (attached DBs, loaded extensions).
        # Cursors don't inherit the USE context, so we set it explicitly.
        cursor = self._conn.cursor()
        if self._active_database:
            cursor.execute(f"USE {self._active_database};")
        return DuckDBJob(query=script.query, connection=cursor, destination=destination)

    def list_existing_table_refs(self) -> set[scripts.TableRef]:
        """List table refs in the DuckLake catalog by querying its metadata database directly.

        This avoids triggering catalog scans on attached databases (e.g. the BigQuery
        extension) by opening a separate read-only connection to the .ducklake file.

        """
        if not self._catalog_path:
            return set()
        meta_conn = duckdb.connect(self._catalog_path, read_only=True)
        try:
            rows = meta_conn.execute(
                "SELECT s.schema_name, t.table_name "
                "FROM ducklake_table t "
                "JOIN ducklake_schema s ON t.schema_id = s.schema_id "
                "WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL"
            ).fetchall()
        finally:
            meta_conn.close()
        return {DuckDBDialect.parse_table_ref(f"{schema}.{table}") for schema, table in rows}

    @property
    def _tables_query(self) -> str:
        db_filter = (
            f"AND database_name = '{self._active_database}'" if self._active_database else ""
        )
        return f"""
        SELECT table_name, schema_name, estimated_size
        FROM duckdb_tables()
        WHERE NOT STARTS_WITH(table_name, 'ducklake_') {db_filter};
        """
