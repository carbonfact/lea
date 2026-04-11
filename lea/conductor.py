from __future__ import annotations

import concurrent.futures
import datetime as dt
import getpass
import json
import os
import pathlib
import sys

import dotenv

import lea
from lea import databases, scripts
from lea.dag import DAGOfScripts
from lea.databases import DatabaseClient, TableStats
from lea.dialects import BigQueryDialect, DuckDBDialect
from lea.session import Session
from lea.table_ref import AUDIT_TABLE_SUFFIX, TableRef


class Conductor:
    def __init__(
        self,
        scripts_dir: str,
        dataset_name: str | None = None,
        project_name: str | None = None,
        env_file_path: str | None = None,
    ):
        # Load environment variables from .env file
        dotenv.load_dotenv(env_file_path or ".env", verbose=True)

        self.warehouse = databases.Warehouse(os.environ["LEA_WAREHOUSE"].lower())

        self.scripts_dir = pathlib.Path(scripts_dir)
        if not self.scripts_dir.is_dir():
            raise ValueError(f"Directory {self.scripts_dir} not found")

        if dataset_name is None:
            if self.warehouse == databases.Warehouse.BIGQUERY:
                dataset_name = os.environ["LEA_BQ_DATASET_NAME"]
            elif self.warehouse == databases.Warehouse.DUCKDB:
                dataset_name = pathlib.Path(os.environ["LEA_DUCKDB_PATH"]).stem
            elif self.warehouse == databases.Warehouse.MOTHERDUCK:
                dataset_name = pathlib.Path(os.environ["LEA_MOTHERDUCK_DATABASE"]).stem
            elif self.warehouse == databases.Warehouse.DUCKLAKE:
                dataset_name = pathlib.Path(os.environ["LEA_DUCKLAKE_DATA_PATH"]).stem
            else:
                raise ValueError(f"Unsupported warehouse {self.warehouse!r}")
        self.dataset_name = dataset_name

        if project_name is None:
            if self.warehouse == databases.Warehouse.BIGQUERY:
                project_name = os.environ["LEA_BQ_PROJECT_ID"]
            elif self.warehouse in {
                databases.Warehouse.DUCKDB,
                databases.Warehouse.MOTHERDUCK,
                databases.Warehouse.DUCKLAKE,
            }:
                project_name = dataset_name
            else:
                raise ValueError(f"Unsupported warehouse {self.warehouse!r}")
        self.project_name = project_name

        lea.log.info("📝 Reading scripts")
        cache_dir = self.scripts_dir / ".lea_cache"

        if self.warehouse == databases.Warehouse.BIGQUERY:
            self.dag = DAGOfScripts.from_directory(
                scripts_dir=self.scripts_dir,
                sql_dialect=BigQueryDialect(),
                dataset_name=self.dataset_name,
                project_name=self.project_name,
                cache_dir=cache_dir,
            )
        elif self.warehouse in {
            databases.Warehouse.DUCKDB,
            databases.Warehouse.MOTHERDUCK,
            databases.Warehouse.DUCKLAKE,
        }:
            self.dag = DAGOfScripts.from_directory(
                scripts_dir=self.scripts_dir,
                sql_dialect=DuckDBDialect(),
                dataset_name=self.dataset_name,
                project_name=None,
                cache_dir=cache_dir,
            )
        else:
            raise ValueError(f"Unsupported warehouse {self.warehouse!r}")
        n_table = sum(1 for s in self.dag.scripts if not s.is_test)
        lea.log.info(f"{n_table:,d} table scripts")
        if n_test := sum(1 for s in self.dag.scripts if s.is_test):
            lea.log.info(f"{n_test:,d} test scripts")

    def run(
        self,
        select: list[str],
        unselect: list[str],
        production: bool = False,
        dry_run: bool = False,
        restart: bool = False,
        incremental_field_name: str | None = None,
        incremental_field_values: list[str] | None = None,
        print_mode: bool = False,
        quack: bool = False,
        quack_push: bool = False,
    ):
        session = self.prepare_session(
            select=select,
            unselect=unselect,
            production=production,
            dry_run=dry_run,
            incremental_field_name=incremental_field_name,
            incremental_field_values=incremental_field_values,
            print_mode=print_mode,
            quack=quack,
            quack_push=quack_push,
        )

        try:
            self.run_session(session, restart=restart, dry_run=dry_run, quack_push=quack_push)
            if session.any_error_has_occurred:
                return sys.exit(1)
        except KeyboardInterrupt:
            lea.log.error("🛑 Keyboard interrupt")
            session.end()
            return sys.exit(1)

    def prepare_session(
        self,
        select: list[str],
        unselect: list[str],
        production: bool = False,
        dry_run: bool = False,
        incremental_field_name: str | None = None,
        incremental_field_values: list[str] | None = None,
        print_mode: bool = False,
        quack: bool = False,
        quack_push: bool = False,
    ) -> Session:
        # We need to select the scripts we want to run. We do this by querying the DAG.
        selected_table_refs = self.dag.select(*select)
        unselected_table_refs = self.dag.select(*unselect)
        if not selected_table_refs - unselected_table_refs:
            msg = "Nothing found for select " + ", ".join(select)
            if unselect:
                msg += " and unselect: " + ", ".join(unselect)
            lea.log.error(msg)
            return sys.exit(1)

        # We need a dataset to materialize the scripts. If we're in production mode, we use the
        # base dataset. If we're in user mode, we use a dataset named after the user.
        write_dataset = self.dataset_name if production else self.dataset_name_with_username

        # Quack mode: set up DuckLake client and classify scripts
        quack_database_client = None
        native_table_refs = set()
        duck_table_refs = set()
        native_dialect = None
        native_dataset = None
        session_quack_setup_stmts = None
        selected_has_native = False

        if quack:
            from lea.quack import classify_scripts

            lea.log.info("🦆 Quack mode enabled")

            # Classify scripts into native vs duck
            native_table_refs, duck_table_refs = classify_scripts(
                dependency_graph=self.dag.dependency_graph,
                scripts=self.dag.scripts,
            )

            # Remember the native dialect for transpilation
            if self.warehouse == databases.Warehouse.BIGQUERY:
                native_dialect = BigQueryDialect()
            else:
                native_dialect = DuckDBDialect()
            native_dataset = self.dataset_name

            # Create a DuckLake client for duck scripts
            quack_catalog = os.environ.get("LEA_QUACK_DUCKLAKE_CATALOG_DATABASE")
            quack_data_path = os.environ.get("LEA_QUACK_DUCKLAKE_DATA_PATH")
            if not quack_catalog or not quack_data_path:
                raise RuntimeError(
                    "Quack mode requires LEA_QUACK_DUCKLAKE_CATALOG_DATABASE and "
                    "LEA_QUACK_DUCKLAKE_DATA_PATH to be set"
                )
            quack_database_client = databases.DuckLakeClient(
                database_path=pathlib.Path(quack_data_path),
                catalog_path=quack_catalog,
                dry_run=dry_run,
                print_mode=print_mode,
            )
            quack_database_client.create_dataset(write_dataset)

            # Set up DuckLake
            conn = quack_database_client.connection
            if gcs_key_id := os.environ.get("LEA_QUACK_DUCKLAKE_GCS_KEY_ID"):
                gcs_secret = os.environ["LEA_QUACK_DUCKLAKE_GCS_SECRET"]
                conn.execute(
                    f"CREATE SECRET (TYPE gcs, KEY_ID '{gcs_key_id}', SECRET '{gcs_secret}');"
                )
            if s3_endpoint := os.environ.get("LEA_QUACK_DUCKLAKE_S3_ENDPOINT"):
                lea.log.info(f"🦆 Setting S3 endpoint to {s3_endpoint!r}")
                conn.execute(f"SET s3_endpoint='{s3_endpoint}'")

            conn.execute(
                f"""
                ATTACH 'ducklake:{quack_catalog}' AS quack_ducklake (
                    DATA_PATH '{quack_data_path}'
                );
                USE quack_ducklake;
                """
            )
            quack_database_client.set_active_database("quack_ducklake")

            # Store setup SQL for lazy loading — the native DB extension is only
            # loaded when actually needed (pulling deps or running native scripts).
            # When quack_push is enabled, attach writably from the start so we can
            # push duck tables back to the native DB later without re-attaching.
            session_quack_setup_stmts = native_dialect.quack_setup_sql(
                env=dict(os.environ), dataset=write_dataset, read_only=not quack_push
            )

            # Create schemas in DuckLake
            schema_names = set(
                table_ref.schema[0] for table_ref in duck_table_refs if table_ref.schema
            )
            for schema_name in schema_names:
                quack_database_client.create_schema(schema_name)

            selected_has_native = bool(
                (selected_table_refs - unselected_table_refs) & native_table_refs
            )

        # Create the native DB client. In quack mode with only duck scripts selected,
        # we defer this entirely — creating it involves network calls (auth, API calls)
        # that are wasteful when all deps are already in DuckLake.
        database_client = None
        existing_tables = {}
        existing_audit_tables = {}

        if not quack or selected_has_native:
            database_client = self.make_client(
                dry_run=dry_run, print_mode=print_mode, production=production
            )
            database_client.create_dataset(write_dataset)

            if isinstance(database_client, databases.DuckDBClient):
                if self.warehouse == databases.Warehouse.DUCKDB:
                    lea.log.info(
                        f"🔩 Using DuckDB database at {database_client.database_path.absolute()}"
                    )
                elif self.warehouse == databases.Warehouse.MOTHERDUCK:
                    database_client.connection.execute("ATTACH 'md:';")
                    database_client.connection.execute(
                        f"CREATE DATABASE IF NOT EXISTS {write_dataset};"
                    )
                    database_client.connection.execute(f"USE {write_dataset};")
                elif self.warehouse == databases.Warehouse.DUCKLAKE:
                    if r2_key_id := os.environ.get("LEA_DUCKLAKE_R2_KEY_ID"):
                        r2_secret = os.environ["LEA_DUCKLAKE_R2_SECRET"]
                        r2_account_id = os.environ["LEA_DUCKLAKE_R2_ACCOUNT_ID"]
                        database_client.connection.execute(
                            f"CREATE SECRET (TYPE r2, KEY_ID '{r2_key_id}', SECRET '{r2_secret}', ACCOUNT_ID '{r2_account_id}');"
                        )
                    if gcs_key_id := os.environ.get("LEA_DUCKLAKE_GCS_KEY_ID"):
                        gcs_secret = os.environ["LEA_DUCKLAKE_GCS_SECRET"]
                        database_client.connection.execute(
                            f"CREATE SECRET (TYPE gcs, KEY_ID '{gcs_key_id}', SECRET '{gcs_secret}');"
                        )
                    if s3_endpoint := os.environ.get("LEA_DUCKLAKE_S3_ENDPOINT"):
                        lea.log.info(f"🔩 Setting S3 endpoint to {s3_endpoint!r}")
                        database_client.connection.execute(f"SET s3_endpoint='{s3_endpoint}'")
                    database_client.connection.execute(
                        f"""
                        ATTACH 'ducklake:{os.environ["LEA_DUCKLAKE_CATALOG_DATABASE"]}' AS my_ducklake (
                            DATA_PATH '{os.environ["LEA_DUCKLAKE_DATA_PATH"]}'
                        );
                        USE my_ducklake;
                        """
                    )
                    database_client.set_active_database("my_ducklake")

                # When using DuckDB, we need to create schema for the tables
                for extension in os.environ.get("LEA_DUCKDB_EXTENSIONS", "").split(","):
                    extension = extension.strip()
                    if extension:
                        lea.log.info(f"🔩 Loading extension {extension}")
                        database_client.connection.execute(f"INSTALL '{extension}';")
                        database_client.connection.execute(f"LOAD '{extension}';")

                lea.log.info("🔩 Creating schemas")
                schema_names = set(
                    table_ref.schema[0]
                    for table_ref in selected_table_refs | unselected_table_refs
                    if table_ref.schema is not None
                )
                for schema_name in schema_names:
                    database_client.create_schema(schema_name)

            existing_tables = self.list_existing_tables(
                database_client=database_client, dataset=write_dataset
            )
            lea.log.info(f"{len(existing_tables):,d} tables already exist")
            existing_audit_tables = self.list_existing_audit_tables(
                database_client=database_client, dataset=write_dataset
            )
            if existing_audit_tables:
                lea.log.info(f"{len(existing_audit_tables):,d} audit tables already exist")

        session = Session(
            database_client=database_client,
            base_dataset=self.dataset_name,
            write_dataset=write_dataset,
            scripts=self.dag.scripts,
            selected_table_refs=selected_table_refs,
            unselected_table_refs=unselected_table_refs,
            existing_tables=existing_tables,
            existing_audit_tables=existing_audit_tables,
            incremental_field_name=incremental_field_name,
            incremental_field_values=incremental_field_values,
            quack_database_client=quack_database_client,
            native_table_refs=native_table_refs,
            duck_table_refs=duck_table_refs,
            native_dialect=native_dialect,
            native_dataset=native_dataset,
            quack_extension_setup_stmts=session_quack_setup_stmts,
        )

        return session

    def run_session(self, session: Session, restart: bool, dry_run: bool, quack_push: bool = False):
        if restart:
            delete_audit_tables(session)

        # Loop over table references in topological order
        materialize_scripts(dag=self.dag, session=session, restart=restart)

        # At this point, the scripts have been materialized into side-tables which we call "audit"
        # tables. We can now take care of promoting the audit tables to production.
        if not session.any_error_has_occurred and not dry_run:
            promote_audit_tables(session)

        # If all the scripts succeeded, we can delete the audit tables.
        if not session.any_error_has_occurred and not dry_run:
            delete_audit_tables(session)

            # Let's also delete orphan tables, which are tables that exist but who's scripts have
            # been deleted.
            delete_orphan_tables(session)

        # In quack-push mode, push promoted duck tables to the native database
        if (
            not session.any_error_has_occurred
            and not dry_run
            and quack_push
            and session.is_quack_mode
        ):
            push_ducklake_to_native(session)

        # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
        session.end()
        assert session.ended_at is not None
        duration_str = str(session.ended_at - session.started_at).split(".")[0]
        emoji = "✅" if not session.any_error_has_occurred else "❌"
        msg = f"{emoji} Finished"
        if session.ended_at - session.started_at > dt.timedelta(seconds=1):
            msg += f", took {duration_str}"
        else:
            msg += ", took less than a second 🚀"
        if session.total_billed_dollars > 0:
            msg += f", cost ${session.total_billed_dollars:.2f}"
        lea.log.info(msg)

    def make_client(
        self, dry_run: bool = False, print_mode: bool = False, production: bool = False
    ) -> (
        databases.BigQueryClient
        | databases.DuckDBClient
        | databases.MotherDuckClient
        | databases.DuckLakeClient
    ):
        if self.warehouse == databases.Warehouse.BIGQUERY:
            # Do imports here to avoid loading them all the time
            from google.oauth2 import service_account

            scopes_str = os.environ.get("LEA_BQ_SCOPES", "https://www.googleapis.com/auth/bigquery")
            scopes = scopes_str.split(",")
            scopes = [scope.strip() for scope in scopes]

            credentials = (
                service_account.Credentials.from_service_account_info(
                    json.loads(bq_service_account_info_str, strict=False), scopes=scopes
                )
                if (bq_service_account_info_str := os.environ.get("LEA_BQ_SERVICE_ACCOUNT"))
                is not None
                else None
            )
            client = databases.BigQueryClient(
                credentials=credentials,
                location=os.environ["LEA_BQ_LOCATION"],
                write_project_id=os.environ["LEA_BQ_PROJECT_ID"],
                compute_project_id=os.environ.get(
                    "LEA_BQ_COMPUTE_PROJECT_ID",
                    credentials.project_id if credentials is not None else None,
                ),
                script_specific_compute_project_ids=parse_bigquery_script_specific_compute_project_ids(
                    env_var=os.environ.get("LEA_BQ_SCRIPT_SPECIFIC_COMPUTE_PROJECT_IDS"),
                    dataset_name=(
                        self.dataset_name if production else self.dataset_name_with_username
                    ),
                    write_project_id=os.environ["LEA_BQ_PROJECT_ID"],
                ),
                storage_billing_model=os.environ.get("LEA_BQ_STORAGE_BILLING_MODEL", "PHYSICAL"),
                dry_run=dry_run,
                print_mode=print_mode,
                default_clustering_fields=[
                    clustering_field.strip()
                    for clustering_field in os.environ.get(
                        "LEA_BQ_DEFAULT_CLUSTERING_FIELDS", ""
                    ).split(",")
                    if clustering_field.strip()
                ],
                big_blue_pick_api_url=os.environ.get("LEA_BQ_BIG_BLUE_PICK_API_URL"),
                big_blue_pick_api_key=os.environ.get("LEA_BQ_BIG_BLUE_PICK_API_KEY"),
                big_blue_pick_api_on_demand_project_id=os.environ.get(
                    "LEA_BQ_BIG_BLUE_PICK_API_ON_DEMAND_PROJECT_ID"
                ),
                big_blue_pick_api_reservation_project_id=os.environ.get(
                    "LEA_BQ_BIG_BLUE_PICK_API_REVERVATION_PROJECT_ID"
                ),
            )
            if client.big_blue_pick_api is not None:
                lea.log.info("🧔‍♂️ Using Big Blue Pick API")
            return client

        elif self.warehouse == databases.Warehouse.DUCKDB:
            return databases.DuckDBClient(
                database_path=pathlib.Path(os.environ["LEA_DUCKDB_PATH"]),
                dry_run=dry_run,
                print_mode=print_mode,
            )

        elif self.warehouse == databases.Warehouse.MOTHERDUCK:
            return databases.MotherDuckClient(
                database_path=pathlib.Path(os.environ["LEA_MOTHERDUCK_DATABASE"]),
                dry_run=dry_run,
                print_mode=print_mode,
            )

        elif self.warehouse == databases.Warehouse.DUCKLAKE:
            return databases.DuckLakeClient(
                database_path=pathlib.Path(os.environ["LEA_DUCKLAKE_DATA_PATH"]),
                dry_run=dry_run,
                print_mode=print_mode,
            )

        raise ValueError(f"Unsupported warehouse {self.warehouse!r}")

    @property
    def dataset_name_with_username(self) -> str:
        username = os.environ.get("LEA_USERNAME", getpass.getuser())
        return f"{self.dataset_name}_{username}"

    def list_existing_tables(
        self, database_client: DatabaseClient, dataset: str
    ) -> dict[TableRef, TableStats]:
        existing_tables = database_client.list_table_stats(dataset)
        existing_tables = {
            table_ref: stats
            for table_ref, stats in existing_tables.items()
            if not table_ref.name.endswith(AUDIT_TABLE_SUFFIX)
        }
        return existing_tables

    def list_existing_audit_tables(
        self, database_client: DatabaseClient, dataset: str
    ) -> dict[TableRef, TableStats]:
        existing_audit_tables = database_client.list_table_stats(dataset)
        existing_audit_tables = {
            table_ref: stats
            for table_ref, stats in existing_audit_tables.items()
            if table_ref.name.endswith(AUDIT_TABLE_SUFFIX)
        }
        return existing_audit_tables


def pull_dependencies_into_ducklake(
    table_refs_to_run: set[TableRef],
    dag: DAGOfScripts,
    session: Session,
    restart: bool = False,
):
    """Pull missing dependencies into DuckLake so duck scripts can read them locally."""
    from lea.quack import determine_deps_to_pull

    # First pass: find candidates without querying DuckLake (pure Python, instant)
    candidates = determine_deps_to_pull(
        table_refs_to_run=table_refs_to_run,
        duck_table_refs=session.duck_table_refs,
        dependency_graph=dag.dependency_graph,
        scripts=dag.scripts,
    )
    if not candidates:
        return

    if restart:
        # On restart, always re-pull everything to get fresh data
        deps_to_pull = candidates
    else:
        # Second pass: filter out candidates that already exist in DuckLake
        if session.quack_database_client is None:
            raise RuntimeError("quack_database_client is required for dependency pulling")
        existing_duck_tables = session.quack_database_client.list_existing_table_refs()
        deps_to_pull = determine_deps_to_pull(
            table_refs_to_run=table_refs_to_run,
            duck_table_refs=session.duck_table_refs,
            dependency_graph=dag.dependency_graph,
            scripts=dag.scripts,
            existing_duck_tables=existing_duck_tables,
        )

        # Mark candidates that already exist in DuckLake as "pulled" so the rewriting
        # uses DuckLake format instead of the BQ extension
        already_in_ducklake = candidates - deps_to_pull
        if already_in_ducklake:
            session.pulled_table_refs |= already_in_ducklake

    if not deps_to_pull:
        return

    # Filter to deps that exist in the native DB, if we have the info.
    # When the native client hasn't been created (pure duck quack mode), we skip
    # the check and attempt to pull everything — the BQ extension will error on
    # any missing tables and we'll log the failure.
    if session.database_client is not None:
        if not session.existing_tables:
            session.existing_tables = session.database_client.list_table_stats(
                session.write_dataset
            )
        existing_native_refs = {
            table_ref.replace_dataset(session.base_dataset) for table_ref in session.existing_tables
        }
        pullable = {
            dep
            for dep in deps_to_pull
            if dep.replace_dataset(session.base_dataset) in existing_native_refs
        }
        unpullable = deps_to_pull - pullable
        if unpullable:
            for dep in unpullable:
                lea.log.warning(
                    f"🦆 Cannot pull {dep} — not found in "
                    f"{session.warehouse.display_name if session.warehouse else 'native DB'}. "
                    "Run upstream dependencies first."
                )
        if not pullable:
            return
    else:
        pullable = deps_to_pull

    session.ensure_quack_extension_loaded()
    ducklake_name = session.format_warehouse_name(databases.Warehouse.DUCKLAKE)
    lea.log.info(f"🦆 Pulling {len(pullable):,d} dependencies into {ducklake_name}")
    if session.quack_database_client is None:
        raise RuntimeError("quack_database_client is required for pulling into DuckLake")
    quack_client = session.quack_database_client
    conn = quack_client.connection
    if quack_client._active_database:
        conn.execute(f"USE {quack_client._active_database};")

    # Ensure schemas exist for pulled deps
    pull_schemas = set(dep.schema[0] for dep in pullable if dep.schema)
    for schema_name in pull_schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def _pull_one(dep: TableRef):
        import time

        if session.native_dialect is None:
            raise RuntimeError("native_dialect is required for pulling dependencies")

        # Source: read from native DB via extension (e.g. bq.citibike_max.core__trips)
        source_ref = dep.replace_dataset(session.write_dataset)
        source_str = session.native_dialect.format_table_ref_for_duckdb(source_ref)
        source_str_display = session.native_dialect.format_table_ref(source_ref)

        # Destination: DuckLake table (e.g. core.trips)
        dest_ref = dep.replace_dataset(None).replace_project(None)
        dest_str = DuckDBDialect.format_table_ref(dest_ref)

        lea.log.info(f"PULLING {source_str_display} → {dest_str}")
        t0 = time.perf_counter()
        cursor = conn.cursor()
        if quack_client._active_database:
            cursor.execute(f"USE {quack_client._active_database};")
        cursor.execute(f"CREATE OR REPLACE TABLE {dest_str} AS SELECT * FROM {source_str}")
        row = cursor.execute(f"SELECT COUNT(*) FROM {dest_str}").fetchone()
        n_rows = row[0] if row else 0
        cursor.close()
        elapsed = time.perf_counter() - t0
        lea.log.info(
            f"[green]SUCCESS[/green] {source_str_display} → {dest_str}, {n_rows:,d} rows ({elapsed:.1f}s)"
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_pull_one, dep): dep for dep in pullable}
        for future in concurrent.futures.as_completed(futures):
            if (exception := future.exception()) is not None:
                dep = futures[future]
                dest_ref = dep.replace_dataset(None).replace_project(None)
                dest_str = DuckDBDialect.format_table_ref(dest_ref)
                lea.log.error(f"[red]ERRORED[/red] {dest_str}\n{exception}")

    session.pulled_table_refs |= pullable


def push_ducklake_to_native(session: Session):
    """Push promoted duck tables from DuckLake to the native database."""
    from lea.dialects import DuckDBDialect

    duck_tables_to_push = {
        table_ref
        for table_ref in session.selected_table_refs
        if table_ref in session.duck_table_refs and not table_ref.is_test
    }

    if not duck_tables_to_push:
        return

    ducklake_name = session.format_warehouse_name(databases.Warehouse.DUCKLAKE)
    warehouse_name = (
        session.format_warehouse_name(session.warehouse) if session.warehouse else "native DB"
    )
    lea.log.info(
        f"🦆 Pushing {len(duck_tables_to_push):,d} {ducklake_name} tables to {warehouse_name}"
    )

    # Ensure the extension is loaded (it's attached writably when quack_push is set)
    session.ensure_quack_extension_loaded()

    if session.quack_database_client is None:
        raise RuntimeError("quack_database_client is required for quack push")
    if session.native_dialect is None:
        raise RuntimeError("native_dialect is required for quack push")

    quack_client = session.quack_database_client
    native_dialect = session.native_dialect

    conn = quack_client.connection
    if quack_client._active_database:
        conn.execute(f"USE {quack_client._active_database};")

    def _push_one(table_ref: TableRef):
        import time

        # Source: promoted DuckLake table (e.g. core.trips)
        duck_ref = table_ref.replace_dataset(None).replace_project(None)
        source_str = DuckDBDialect.format_table_ref(duck_ref)

        # Destination: native DB via writable extension (e.g. bq.dataset.core__trips)
        dest_ref = table_ref.replace_dataset(session.write_dataset)
        dest_str = native_dialect.format_table_ref_for_duckdb(dest_ref)
        dest_str_display = native_dialect.format_table_ref(dest_ref)

        lea.log.info(f"PUSHING {source_str} → {dest_str_display}")
        t0 = time.perf_counter()
        cursor = conn.cursor()
        if quack_client._active_database:
            cursor.execute(f"USE {quack_client._active_database};")
        cursor.execute(f"CREATE OR REPLACE TABLE {dest_str} AS SELECT * FROM {source_str}")
        row = cursor.execute(f"SELECT COUNT(*) FROM {source_str}").fetchone()
        n_rows = row[0] if row else 0
        cursor.close()
        elapsed = time.perf_counter() - t0
        lea.log.info(
            f"[green]SUCCESS[/green] {source_str} → {dest_str_display}, {n_rows:,d} rows ({elapsed:.1f}s)"
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_push_one, ref): ref for ref in duck_tables_to_push}
        for future in concurrent.futures.as_completed(futures):
            if (exception := future.exception()) is not None:
                ref = futures[future]
                duck_ref = ref.replace_dataset(None).replace_project(None)
                source_str = DuckDBDialect.format_table_ref(duck_ref)
                lea.log.error(f"[red]ERRORED[/red] pushing {source_str}\n{exception}")


def materialize_scripts(dag: DAGOfScripts, session: Session, restart: bool = False):
    table_refs_to_run = determine_table_refs_to_run(
        selected_table_refs=session.selected_table_refs,
        unselected_table_refs=session.unselected_table_refs,
        existing_audit_tables=session.existing_audit_tables,
        dag=dag,
        base_dataset=session.base_dataset,
    )
    if not table_refs_to_run:
        lea.log.info("✅ Nothing needs materializing")
        return
    # In quack mode, pull missing dependencies into DuckLake before running
    if session.is_quack_mode:
        pull_dependencies_into_ducklake(
            table_refs_to_run=table_refs_to_run,
            dag=dag,
            session=session,
            restart=restart,
        )
        # If any native scripts are being run, duck scripts may reference their
        # audit tables via the native DB extension (e.g. bq.dataset.table___audit)
        if table_refs_to_run & session.native_table_refs:
            session.ensure_quack_extension_loaded()

    def collect_completed():
        """Wait for at least one in-flight script to complete, process all that are done.

        Returns the number of native scripts that completed successfully.

        """
        from lea.session import ScriptError

        done, _ = concurrent.futures.wait(
            session.run_script_futures, return_when=concurrent.futures.FIRST_COMPLETED
        )
        native_completed = 0
        for future in done:
            script_done = session.run_script_futures[future]
            table_ref = session.remove_write_context_from_table_ref(script_done.table_ref)
            if exception := future.exception():
                # ScriptError is already logged by monitor_job, only log unexpected errors
                if not isinstance(exception, ScriptError):
                    lea.log.error(f"[red]Failed running {script_done.table_ref}\n{exception}[/red]")
            else:
                # Only mark successful scripts as done in the DAG, so that
                # dependents of errored scripts are never unlocked.
                dag.done(table_ref)
                if table_ref in session.native_table_refs:
                    native_completed += 1
            session.run_script_futures_complete[future] = session.run_script_futures.pop(future)
        return native_completed

    def has_duck_in_flight():
        return any(
            session.remove_write_context_from_table_ref(s.table_ref)
            not in session.native_table_refs
            for s in session.run_script_futures.values()
        )

    lea.log.info(f"🔵 Running {len(table_refs_to_run):,d} scripts")
    dag.prepare()
    native_completed_since_refresh = 0
    while dag.is_active():
        if session.any_error_has_occurred:
            lea.log.error("✋ Early ending because an error occurred")
            break

        # If native scripts have completed since the last refresh, we need to
        # refresh the BQ extension so duck scripts can see newly created audit
        # tables. We must wait for duck scripts to drain first because they share
        # the DuckDB connection — detach/reattach while a query is running errors.
        if session.is_quack_mode and native_completed_since_refresh > 0:
            if has_duck_in_flight():
                # Don't pull new scripts from the DAG yet — wait for in-flight
                # duck scripts to finish, then refresh on the next iteration.
                native_completed_since_refresh += collect_completed()
                continue
            _refresh_quack_extension(session)
            native_completed_since_refresh = 0

        # Start available jobs
        for script_to_run in dag.iter_scripts(table_refs_to_run):
            if session.any_error_has_occurred:
                break
            # Before executing a script, we need to contextualize it. We have to edit its
            # dependencies, add incremental logic, and set the write context.
            script_to_run = session.add_context_to_script(script_to_run)
            # 🔨 if you're developping on lea, you can call session.run_script(script_to_run) here
            # to get a better stack trace. This is because the executor will run the script in a
            # different thread, and the exception will be raised in that thread, not in the main
            # thread.
            future = session.executor.submit(session.run_script, script_to_run)
            session.run_script_futures[future] = script_to_run

        if not session.run_script_futures:
            continue

        native_completed_since_refresh += collect_completed()


def _refresh_quack_extension(session: Session):
    """Detach and reattach the native DB extension to refresh its metadata cache.

    The DuckDB BigQuery extension caches table metadata on attach. After native
    scripts create new audit tables in BigQuery, the extension can't see them.
    Detaching and reattaching forces a cache refresh.

    """
    if session.native_dialect is None or session.quack_database_client is None:
        return

    attached_name = session.native_dialect.quack_attached_name
    if not attached_name:
        return

    conn = session.quack_database_client.connection
    warehouse_name = (
        session.format_warehouse_name(session.warehouse) if session.warehouse else attached_name
    )
    lea.log.info(f"🦆 Refreshing {warehouse_name} extension metadata cache")
    conn.execute(f"DETACH {attached_name};")
    # Re-run only the ATTACH statement (extension is already installed/loaded)
    attach_stmt = session._quack_extension_setup_stmts[-1]
    conn.execute(attach_stmt)


def promote_audit_tables(session: Session):
    lea.log.info("🟢 Promoting audit tables")
    # Ideally, we would like to do this automatically, but BigQuery does not support DDL
    # statements in a transaction. So we do it concurrently. This isn't ideal, but it's the
    # best we can do for now. There's a very small chance that at least one promotion job will
    # fail.
    # https://hiflylabs.com/blog/2022/11/22/dbt-deployment-best-practices
    # https://calogica.com/sql/bigquery/dbt/2020/05/24/dbt-bigquery-blue-green-wap.html
    # https://calogica.com/assets/wap_dbt_bigquery.pdf
    # Note: it's important for the following loop to be a list comprehension. If we used a
    # generator expression, the loop would be infinite because jobs are being added to
    # session.jobs when session.promote is called.
    for selected_table_ref in session.selected_table_refs:
        if selected_table_ref.is_test:
            continue

        is_duck = session.is_quack_mode and selected_table_ref in session.duck_table_refs
        if is_duck:
            # Duck tables are promoted in DuckLake using DuckDB-format table refs
            duck_ref = (
                selected_table_ref.replace_dataset(None).replace_project(None).add_audit_suffix()
            )
            future = session.executor.submit(
                session.promote_audit_table, duck_ref, session.quack_database_client
            )
            session.promote_audit_tables_futures[future] = duck_ref
        else:
            selected_table_ref = session.add_write_context_to_table_ref(selected_table_ref)
            future = session.executor.submit(session.promote_audit_table, selected_table_ref)
            session.promote_audit_tables_futures[future] = selected_table_ref

    # Wait for all promotion jobs to finish
    for future in concurrent.futures.as_completed(session.promote_audit_tables_futures):
        if (exception := future.exception()) is not None:
            lea.log.error(f"Promotion failed\n{exception}")


def delete_audit_tables(session: Session):
    # Depending on when delete_audit_tables is called, there might be new audit tables that have
    # been created. We need to delete them too. We do this by adding the write context to the
    # table references. This will add the audit suffix to the table reference, which will make
    # it match the audit tables that have been created.
    table_refs_to_delete = set(session.existing_audit_tables) | {
        session.add_write_context_to_table_ref(table_ref)
        for table_ref in session.selected_table_refs
    }

    # In quack mode, split duck vs native audit tables
    if session.is_quack_mode:
        duck_audit_refs = {
            table_ref.replace_dataset(None).replace_project(None).add_audit_suffix()
            for table_ref in session.selected_table_refs
            if table_ref in session.duck_table_refs
        }
        native_audit_refs = table_refs_to_delete - {
            session.add_write_context_to_table_ref(table_ref)
            for table_ref in session.selected_table_refs
            if table_ref in session.duck_table_refs
        }
        if native_audit_refs and session.database_client is not None:
            warehouse_name = (
                session.format_warehouse_name(session.warehouse) if session.warehouse else "native"
            )
            lea.log.info(f"🧹 Deleting {warehouse_name} audit tables")
            delete_table_refs(
                table_refs=native_audit_refs,
                database_client=session.database_client,
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
                verbose=False,
            )
        if duck_audit_refs and session.quack_database_client is not None:
            lea.log.info(
                f"🧹 Deleting {session.format_warehouse_name(databases.Warehouse.DUCKLAKE)} audit tables"
            )
            delete_table_refs(
                table_refs=duck_audit_refs,
                database_client=session.quack_database_client,
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
                verbose=False,
            )
    elif table_refs_to_delete and session.database_client is not None:
        warehouse_name = (
            session.format_warehouse_name(session.warehouse) if session.warehouse else "native"
        )
        lea.log.info(f"🧹 Deleting {warehouse_name} audit tables")
        delete_table_refs(
            table_refs=table_refs_to_delete,
            database_client=session.database_client,
            executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
            verbose=False,
        )
    session.existing_audit_tables = {}


def delete_orphan_tables(session: Session):
    table_refs_to_delete = set(session.existing_tables) - {
        session.add_write_context_to_table_ref(table_ref).remove_audit_suffix()
        for table_ref in session.scripts
    }
    if table_refs_to_delete and session.database_client is not None:
        lea.log.info("🧹 Deleting orphan tables")
        delete_table_refs(
            table_refs=table_refs_to_delete,
            database_client=session.database_client,
            executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
            verbose=True,
        )
        session.existing_audit_tables = {}


def delete_table_refs(
    table_refs: set[TableRef],
    database_client: DatabaseClient,
    executor: concurrent.futures.ThreadPoolExecutor,
    verbose: bool,
):
    futures: dict[concurrent.futures.Future, TableRef] = {}
    for table_ref in table_refs:
        future = executor.submit(database_client.delete_table, table_ref)
        futures[future] = table_ref

    for future in concurrent.futures.as_completed(futures):
        if (exception := future.exception()) is not None:
            lea.log.error(exception)
            continue
        if verbose:
            lea.log.info(f"Deleted {futures[future]}")


def determine_table_refs_to_run(
    selected_table_refs: set[TableRef],
    unselected_table_refs: set[TableRef],
    existing_audit_tables: dict[TableRef, TableStats],
    dag: DAGOfScripts,
    base_dataset: str,
) -> set[TableRef]:
    """Determine which table references need to be run.

    We want to:

    1. Run tables that have been selected. This is obtained from the DAGOfScripts.select method.
    2. Skip tables that already exist. This is obtained from the database client.
    3. Don't skip tables that have been edited since last being run. This is obtained from the
       scripts themselves.

    This last requirement is why we need an extra method to determine which table references need
    to be run. We compare the updated_at of the script with the updated_at of the corresponding
    table (if it exists): a script that has been modified since the last time it was run needs to
    be run again. All the descendants of this script also need to be run.

    On top of this, we also include each test script that is associated with the selected table
    references. We do this because it's a good default behavior.

    """
    table_refs_to_run = selected_table_refs.copy()

    # By default, we do not run scripts that have an audit table materialized. We will determine
    # afterwards, based on each script's modified_at, if we need to run them again.
    existing_audit_table_refs = {
        table_ref.remove_audit_suffix().replace_dataset(base_dataset): stats
        for table_ref, stats in existing_audit_tables.items()
    }
    table_refs_to_run -= set(existing_audit_table_refs)

    # Now we check if any of the audit tables have had their script modified since the last time
    # they were materialized. If so, we need to run them again, as well as their descendants.
    for table_ref in selected_table_refs & set(existing_audit_table_refs):
        script = dag.scripts[table_ref]
        if script.updated_at > existing_audit_table_refs[table_ref].updated_at:  # type: ignore
            lea.log.info(f"📝 {table_ref} was modified, re-materializing it")
            table_refs_to_run.add(table_ref)
            table_refs_to_run |= set(dag.iter_descendants(table_ref)) & selected_table_refs

    # Include applicable tests. That is, test scripts whose dependencies are all in the set of
    # selected table references.
    applicable_test_scripts_table_refs = {
        script.table_ref
        for script in dag.scripts.values()
        if script.is_test
        and all(dependency in table_refs_to_run for dependency in script.dependencies)
    }
    table_refs_to_run |= applicable_test_scripts_table_refs

    # Now we remove the unselected table references from the set of table references to run. We do
    # this at the very end, because of the above logic which adds table references to the set of
    # table references to run. For instance, if we run
    #
    # lea --select core.accounts --unselect tests
    #
    # we don't want the tests which are applicable to core.accounts to be run.
    table_refs_to_run -= unselected_table_refs

    return table_refs_to_run


def parse_bigquery_script_specific_compute_project_ids(
    env_var: str | None,
    dataset_name: str,
    write_project_id: str,
) -> dict[scripts.TableRef, str]:
    if env_var is None:
        return {}
    mapping = json.loads(env_var)
    return {
        (
            BigQueryDialect.parse_table_ref(table_ref_str)
            .replace_dataset(dataset_name)
            .replace_project(write_project_id)
            .add_audit_suffix()
        ): compute_project_id
        for table_ref_str, compute_project_id in mapping.items()
    }
