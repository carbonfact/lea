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
from lea import databases
from lea.dag import DAGOfScripts
from lea.databases import DatabaseClient, TableStats
from lea.dialects import BigQueryDialect, DuckDBDialect
from lea.session import Session
from lea.table_ref import AUDIT_TABLE_SUFFIX, TableRef


class Conductor:
    def __init__(
        self, scripts_dir: str, dataset_name: str | None = None, project_name: str | None = None
    ):
        # Load environment variables from .env file
        # TODO: is is Pythonic to do this here?
        dotenv.load_dotenv(".env", verbose=True)

        self.warehouse = os.environ["LEA_WAREHOUSE"].lower()

        self.scripts_dir = pathlib.Path(scripts_dir)
        if not self.scripts_dir.is_dir():
            raise ValueError(f"Directory {self.scripts_dir} not found")

        if dataset_name is None:
            if self.warehouse == "bigquery":
                dataset_name = os.environ.get("LEA_BQ_DATASET_NAME")

            if self.warehouse == "duckdb":
                duckdb_path = pathlib.Path(os.environ.get("LEA_DUCKDB_PATH", ""))
                dataset_name = duckdb_path.stem
        if dataset_name is None:
            raise ValueError("Dataset name could not be inferred")
        self.dataset_name = dataset_name

        if project_name is None:
            if self.warehouse == "bigquery":
                project_name = os.environ.get("LEA_BQ_PROJECT_ID")
            if self.warehouse == "duckdb":
                project_name = dataset_name
        if project_name is None:
            raise ValueError("Project name could not be inferred")
        self.project_name = project_name

        lea.log.info("📝 Reading scripts")

        if self.warehouse == "bigquery":
            self.dag = DAGOfScripts.from_directory(
                scripts_dir=self.scripts_dir,
                sql_dialect=BigQueryDialect(),
                dataset_name=self.dataset_name,
                project_name=self.project_name if self.warehouse == "bigquery" else None,
            )
        if self.warehouse == "duckdb":
            self.dag = DAGOfScripts.from_directory(
                scripts_dir=self.scripts_dir,
                sql_dialect=DuckDBDialect(),
                dataset_name=self.dataset_name,
                project_name=None,
            )
        lea.log.info(f"{sum(1 for s in self.dag.scripts if not s.is_test):,d} table scripts")
        lea.log.info(f"{sum(1 for s in self.dag.scripts if s.is_test):,d} test scripts")

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
    ):
        session = self.prepare_session(
            select=select,
            unselect=unselect,
            production=production,
            dry_run=dry_run,
            incremental_field_name=incremental_field_name,
            incremental_field_values=incremental_field_values,
            print_mode=print_mode,
        )

        try:
            self.run_session(session, restart=restart, dry_run=dry_run)
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
    ) -> Session:
        # We need a database client to run scripts
        database_client = self.make_client(dry_run=dry_run, print_mode=print_mode)

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
        write_dataset = self.dataset_name if production else self.name_user_dataset()
        database_client.create_dataset(write_dataset)

        # When using DuckDB, we need to create schema for the tables
        if self.warehouse == "duckdb":
            lea.log.info("🔩 Creating schemas")
            for table_ref in selected_table_refs - unselected_table_refs:
                database_client.create_schema(table_ref)

        # When the scripts run, they are materialized into side-tables which we call "audit"
        # tables. When a run stops because of an error, the audit tables are left behind. If we
        # want to start fresh, we have to delete the audit tables. If not, the materialized tables
        # can be skipped.
        existing_tables = self.list_existing_tables(
            database_client=database_client, dataset=write_dataset
        )
        lea.log.info(f"{len(existing_tables):,d} tables already exist")
        existing_audit_tables = self.list_existing_audit_tables(
            database_client=database_client, dataset=write_dataset
        )

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
        )

        return session

    def run_session(self, session: Session, restart: bool, dry_run: bool):
        if restart:
            delete_audit_tables(session)

        # Loop over table references in topological order
        materialize_scripts(dag=self.dag, session=session)

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

        # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
        session.end()
        duration_str = str(session.ended_at - session.started_at).split(".")[0]  # type: ignore[operator]
        emoji = "✅" if not session.any_error_has_occurred else "❌"
        msg = f"{emoji} Finished"
        if session.ended_at - session.started_at > dt.timedelta(seconds=1):
            msg += f", took {duration_str}"
        else:
            msg += ", took less than a second 🚀"
        if session.total_billed_dollars > 0:
            msg += f", cost ${session.total_billed_dollars:.2f}"
        lea.log.info(msg)

    def make_client(self, dry_run: bool = False, print_mode: bool = False) -> DatabaseClient:
        if self.warehouse.lower() == "bigquery":
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
                storage_billing_model=os.environ.get("LEA_BQ_STORAGE_BILLING_MODEL"),
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

        if self.warehouse.lower() == "duckdb":
            return databases.DuckDBClient(
                database_path=pathlib.Path(os.environ.get("LEA_DUCKDB_PATH", "")),
                dry_run=dry_run,
                print_mode=print_mode,
            )

        raise ValueError(f"Unsupported warehouse {self.warehouse!r}")

    def name_user_dataset(self) -> str:
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


def materialize_scripts(dag: DAGOfScripts, session: Session):
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
    lea.log.info(f"🔵 Running {len(table_refs_to_run):,d} scripts")
    dag.prepare()
    while dag.is_active():
        # If we're in early end mode, we need to check if any script errored, in which case we
        # have to stop everything.
        if session.any_error_has_occurred:
            lea.log.error("✋ Early ending because an error occurred")
            break

        # Start available jobs
        for script_to_run in dag.iter_scripts(table_refs_to_run):
            # Before executing a script, we need to contextualize it. We have to edit its
            # dependencies, add incremental logic, and set the write context.
            script_to_run = session.add_context_to_script(script_to_run)
            # 🔨 if you're developping on lea, you can call session.run_script(script_to_run) here
            # to get a better stack trace. This is because the executor will run the script in a
            # different thread, and the exception will be raised in that thread, not in the main
            # thread.
            future = session.executor.submit(session.run_script, script_to_run)
            session.run_script_futures[future] = script_to_run

        # Check for scripts that have finished
        done, _ = concurrent.futures.wait(
            session.run_script_futures, return_when=concurrent.futures.FIRST_COMPLETED
        )
        for future in done:
            script_done = session.run_script_futures[future]
            if exception := future.exception():
                lea.log.error(f"Failed running {script_done.table_ref}\n{exception}")
            table_ref = session.remove_write_context_from_table_ref(script_done.table_ref)
            session.run_script_futures_complete[future] = session.run_script_futures.pop(future)
            dag.done(table_ref)


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
    if table_refs_to_delete:
        lea.log.info("🧹 Deleting audit tables")
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
    if table_refs_to_delete:
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
        if script.updated_at > existing_audit_table_refs[table_ref].updated_at:
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
