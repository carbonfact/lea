from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import enum
import getpass
import json
import logging
import os
import pathlib
import re
import sys
import threading
import time
from collections.abc import Callable

import click
import dotenv
from rich.logging import RichHandler

from lea import databases
from lea.dag import DAGOfScripts
from lea.databases import DatabaseClient, DatabaseJob, TableStats
from lea.dialects import BigQueryDialect
from lea.field import FieldTag
from lea.scripts import Script
from lea.table_ref import AUDIT_TABLE_SUFFIX, TableRef

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            rich_tracebacks=True,
            show_level=False,
            show_path=False,
            markup=True,
            tracebacks_suppress=[click],
        )
    ],
)

log = logging.getLogger("rich")


class JobStatus(enum.Enum):
    RUNNING = "RUNNING"
    SUCCESS = "[green]SUCCESS[/green]"
    ERRORED = "[red]ERRORED[/red]"
    STOPPED = "[yellow]STOPPED[/yellow]"

    def __str__(self):
        return self.value


@dataclasses.dataclass
class Job:
    table_ref: TableRef
    is_test: bool
    database_job: DatabaseJob
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime | None = None
    status: JobStatus = JobStatus.RUNNING

    def __hash__(self):
        return hash(self.table_ref)


def format_bytes(size: float) -> str:
    # Define the size units in ascending order
    power = 1024
    n = 0
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]

    # Convert bytes to the highest possible unit
    while size >= power and n < len(units) - 1:
        size /= power
        n += 1

    # Format the result with two decimal places
    return f"{size:.0f}{units[n]}"


class Session:
    def __init__(
        self,
        database_client: DatabaseClient,
        base_dataset: str,
        write_dataset: str,
        scripts: dict[TableRef, Script],
        selected_table_refs: set[TableRef],
        existing_audit_tables: set[TableRef],
        incremental_field_name=None,
        incremental_field_values=None,
    ):
        self.database_client = database_client
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs
        self.existing_audit_tables = existing_audit_tables
        self.incremental_field_name = incremental_field_name
        self.incremental_field_values = incremental_field_values

        self.jobs: list[Job] = []
        self.started_at = dt.datetime.now()
        self.ended_at: dt.datetime | None = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=None)
        self.run_script_futures: dict = {}
        self.run_script_futures_complete: dict = {}
        self.promote_audit_tables_futures: dict = {}
        self.stop_event = threading.Event()

        if self.incremental_field_name is not None:
            self.filterable_table_refs = {
                table_ref.replace_dataset(self.write_dataset)
                for table_ref in scripts
                if any(
                    field.name == incremental_field_name
                    for field in scripts[table_ref].fields or []
                )
            }
            self.incremental_table_refs = {
                table_ref.replace_dataset(self.write_dataset)
                for table_ref in selected_table_refs | set(existing_audit_tables)
                if any(
                    field.name == incremental_field_name and FieldTag.INCREMENTAL in field.tags
                    for field in scripts[
                        table_ref.remove_audit_suffix().replace_dataset(self.base_dataset)
                    ].fields
                    or []
                )
            }
        else:
            self.filterable_table_refs = set()
            self.incremental_table_refs = set()

    def add_write_context_to_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.write_dataset)
        table_ref = table_ref.add_audit_suffix()
        return table_ref

    def remove_write_context_from_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.base_dataset)
        table_ref = table_ref.remove_audit_suffix()
        return table_ref

    def add_context_to_script(self, script: Script) -> Script:
        def add_context_to_dependency(dependency: TableRef) -> TableRef | None:
            if dependency.project != script.table_ref.project:
                return None

            if (
                dependency.replace_dataset(self.base_dataset)
                in self.selected_table_refs
                | {
                    self.remove_write_context_from_table_ref(table_ref)
                    for table_ref in self.existing_audit_tables
                }
                and dependency.replace_dataset(self.base_dataset) in self.scripts
            ):
                dependency = dependency.add_audit_suffix()

            dependency = dependency.replace_dataset(self.write_dataset)

            return dependency

        script = replace_script_dependencies(script=script, replace_func=add_context_to_dependency)

        # If a script is marked as incremental, it implies that it can be run incrementally. This
        # means that we have to filter the script's dependencies, as well as filter the output.
        # This logic is implemented by the script's SQL dialect.
        if script.table_ref.replace_dataset(self.write_dataset) in self.incremental_table_refs:
            script = dataclasses.replace(
                script,
                code=script.sql_dialect.add_dependency_filters(
                    code=script.code,
                    incremental_field_name=self.incremental_field_name,
                    incremental_field_values=self.incremental_field_values,
                    # One caveat is the dependencies which are not incremental do not have to be
                    # filtered. Indeed, they are already filtered by the fact that they are
                    # incremental.
                    dependencies_to_filter=self.filterable_table_refs - self.incremental_table_refs,
                ),
            )

        # If the script is not incremental, we're not out of the woods! All scripts are
        # materialized into side-tables which we call "audit" tables. This is the WAP pattern.
        # Therefore, if a script is not incremental, but it depends on an incremental script, we
        # have to modify the script to use both the incremental and non-incremental versions of
        # the dependency. This is handled by the script's SQL dialect.
        elif self.incremental_table_refs:
            script = dataclasses.replace(
                script,
                code=script.sql_dialect.handle_incremental_dependencies(
                    code=script.code,
                    incremental_field_name=self.incremental_field_name,
                    incremental_field_values=self.incremental_field_values,
                    incremental_dependencies={
                        incremental_table_ref: incremental_table_ref.add_audit_suffix()
                        for incremental_table_ref in self.incremental_table_refs
                    },
                ),
            )

        return script.replace_table_ref(self.add_write_context_to_table_ref(script.table_ref))

    def run_script(self, script: Script):
        # If the script is a test, we don't materialize it, we just query it. A test fails if it
        # returns any rows.
        if script.is_test:
            database_job = self.database_client.query_script(script=script)
        # If the script is not a test, it's a regular table, so we materialize it. Instead of
        # directly materializing it to the destination table, we materialize it to a side-table
        # which we call an "audit" table. Once all the scripts have run successfully, we will
        # promote the audit tables to the destination tables. This is the WAP pattern.
        else:
            database_job = self.database_client.materialize_script(script=script)

        job = Job(table_ref=script.table_ref, is_test=script.is_test, database_job=database_job)
        self.jobs.append(job)

        msg = f"{job.status} {script.table_ref}"

        if script.table_ref.remove_audit_suffix() in self.incremental_table_refs:
            msg += " (incremental)"
        log.info(msg)

        self.monitor_job(job)

    def monitor_job(self, job: Job):
        # We're going to do exponential backoff. This is because we don't want to overload
        # whatever API is used to check whether a database job is over or not. We're going to
        # check every second, then every two seconds, then every four seconds, etc. until we
        # reach a maximum delay of 10 seconds.
        base_delay = 1
        max_delay = 10
        retries = 0
        checked_at = dt.datetime.now()

        while not self.stop_event.is_set():
            if not job.database_job.is_done:
                delay = min(max_delay, base_delay * (2**retries))
                retries += 1
                if (now := dt.datetime.now()) - checked_at >= dt.timedelta(seconds=10):
                    duration_str = str(now - job.started_at).split(".")[0]
                    log.info(f"{job.status} {job.table_ref} after {duration_str}")
                    checked_at = now
                time.sleep(delay)
                continue

            # Case 1: the job raised an exception
            if (exception := job.database_job.exception) is not None:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {job.table_ref}\n{exception}")

            # Case 2: the job succeeded, but it's a test and there are negative cases
            elif job.is_test and not (dataframe := job.database_job.result).empty:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {job.table_ref}\n{dataframe.head()}")

            # Case 3: the job succeeded!
            else:
                job.status = JobStatus.SUCCESS
                msg = f"{job.status} {job.table_ref}"
                job.ended_at = dt.datetime.now()
                duration_str = str(job.ended_at - job.started_at).split(".")[0]
                msg += f", took {duration_str}, cost ${job.database_job.billed_dollars:.2f}"
                if not job.is_test:
                    if (stats := job.database_job.statistics) is not None:
                        msg += f", contains {stats.n_rows:,d} rows"
                        msg += f", weighs {format_bytes(stats.n_bytes)}"
                log.info(msg)

            return

    def promote_audit_table(self, table_ref: TableRef):
        from_table_ref = table_ref
        to_table_ref = table_ref.remove_audit_suffix()

        is_incremental = (
            self.incremental_field_name is not None and to_table_ref in self.incremental_table_refs
        )
        if is_incremental:
            database_job = self.database_client.delete_and_insert(
                from_table_ref=from_table_ref,
                to_table_ref=to_table_ref,
                on=self.incremental_field_name,
            )
        else:
            database_job = self.database_client.clone_table(
                from_table_ref=from_table_ref, to_table_ref=to_table_ref
            )

        job = Job(table_ref=to_table_ref, is_test=False, database_job=database_job)
        self.jobs.append(job)
        log.info(f"{job.status} {job.table_ref}" + (" (incremental)" if is_incremental else ""))

        self.monitor_job(job)

    def end(self):
        log.info("😴 Ending session")
        self.stop_event.set()
        for job in self.jobs:
            if job.status == JobStatus.RUNNING:
                job.database_job.stop()
                job.status = JobStatus.STOPPED
                log.info(f"{job.status} {job.table_ref}")
        self.executor.shutdown()
        self.ended_at = dt.datetime.now()

    @property
    def any_error_has_occurred(self) -> bool:
        return any(job.status == JobStatus.ERRORED for job in self.jobs) or any(
            future.exception() is not None for future in self.run_script_futures_complete
        )

    @property
    def total_billed_dollars(self) -> float:
        return sum(job.database_job.billed_dollars for job in self.jobs)


def replace_script_dependencies(
    script: Script, replace_func: Callable[[TableRef], TableRef]
) -> Script:
    """

    It's often necessary to edit the dependencies of a script. For example, we might want
    to change the dataset of a dependency. Or we might want to append a suffix a table name
    when we're doing a write/audit/publish operation.

    """
    code = script.code

    for dependency_to_edit in script.dependencies:
        new_dependency = replace_func(dependency_to_edit)
        if new_dependency is None:
            continue

        dependency_to_edit_without_project_str = script.sql_dialect.format_table_ref(
            dependency_to_edit.replace_project(None)
        )
        new_dependency_str = script.sql_dialect.format_table_ref(new_dependency)
        code = re.sub(
            rf"\b{dependency_to_edit_without_project_str}\b",
            new_dependency_str,
            code,
        )

        # We also have to handle the case where the table is referenced to access a field.
        # TODO: refactor this with the above
        dependency_to_edit_without_dataset = dataclasses.replace(
            dependency_to_edit, dataset="", project=None
        )
        dependency_to_edit_without_dataset_str = script.sql_dialect.format_table_ref(
            dependency_to_edit_without_dataset
        )
        new_dependency_without_dataset = dataclasses.replace(
            new_dependency, dataset="", project=None
        )
        new_dependency_without_dataset_str = script.sql_dialect.format_table_ref(
            new_dependency_without_dataset
        )
        code = re.sub(
            rf"\b{dependency_to_edit_without_dataset_str}\b",
            new_dependency_without_dataset_str,
            code,
        )

    return dataclasses.replace(script, code=code)


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
            log.error(exception)
            continue
        if verbose:
            log.info(f"Deleted {futures[future]}")


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
        if dataset_name is None:
            raise ValueError("Dataset name could not be inferred")
        self.dataset_name = dataset_name

        if project_name is None:
            if self.warehouse == "bigquery":
                project_name = os.environ.get("LEA_BQ_PROJECT_ID")
        if project_name is None:
            raise ValueError("Project name could not be inferred")
        self.project_name = project_name

        log.info("📝 Reading scripts")

        self.dag = DAGOfScripts.from_directory(
            scripts_dir=self.scripts_dir,
            sql_dialect=BigQueryDialect(),
            dataset_name=self.dataset_name,
            project_name=self.project_name,
        )
        log.info(f"{len(self.dag.scripts):,d} scripts found")

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

            return databases.BigQueryClient(
                credentials=credentials,
                location=os.environ["LEA_BQ_LOCATION"],
                write_project_id=os.environ["LEA_BQ_PROJECT_ID"],
                compute_project_id=os.environ.get(
                    "LEA_BQ_COMPUTE_PROJECT_ID",
                    credentials.project_id if credentials is not None else None,
                ),
                dry_run=dry_run,
                print_mode=print_mode,
            )

        raise ValueError(f"Unsupported warehouse {self.warehouse!r}")

    def name_user_dataset(self) -> str:
        username = os.environ.get("LEA_USERNAME", getpass.getuser())
        return f"{self.dataset_name}_{username}"

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
        # We need a database client to run scripts
        database_client = self.make_client(dry_run=dry_run, print_mode=print_mode)

        # We need to select the scripts we want to run. We do this by querying the DAG.
        selected_table_refs = self.dag.select(*select)
        unselected_table_refs = self.dag.select(*unselect)
        selected_table_refs -= unselected_table_refs
        if not selected_table_refs:
            msg = "Nothing found for select " + ", ".join(select)
            if unselect:
                msg += " and unselect: " + ", ".join(unselect)
            log.error(msg)
            return sys.exit(1)
        log.info(f"{len(selected_table_refs):,d} scripts selected")

        # We need a dataset to materialize the scripts. If we're in production mode, we use the
        # base dataset. If we're in user mode, we use a dataset named after the user.
        write_dataset = self.dataset_name if production else self.name_user_dataset()
        database_client.create_dataset(write_dataset)

        # When the scripts run, they are materialized into side-tables which we call "audit"
        # tables. When a run stops because of an error, the audit tables are left behind. If we
        # want to start fresh, we have to delete the audit tables. If not, the materialized tables
        # can be skipped.
        existing_audit_tables = self.list_existing_audit_tables(
            database_client=database_client, dataset=write_dataset
        )
        log.info(f"{len(existing_audit_tables):,d} audit tables already exist")

        session = Session(
            database_client=database_client,
            base_dataset=self.dataset_name,
            write_dataset=write_dataset,
            scripts=self.dag.scripts,
            selected_table_refs=selected_table_refs,
            existing_audit_tables=existing_audit_tables,
            incremental_field_name=incremental_field_name,
            incremental_field_values=incremental_field_values,
        )

        try:
            self.run_session(session, restart=restart, dry_run=dry_run)
            if session.any_error_has_occurred:
                return sys.exit(1)
        except KeyboardInterrupt:
            log.error("🛑 Keyboard interrupt")
            session.end()
            return sys.exit(1)

    def run_session(self, session: Session, restart: bool, dry_run: bool):
        if restart:
            delete_audit_tables(session)

        # Loop over table references in topological order
        run_scripts(dag=self.dag, session=session)

        # At this point, the scripts have been materialized into side-tables which we call "audit"
        # tables. We can now take care of promoting the audit tables to production.
        if not session.any_error_has_occurred and not dry_run:
            promote_audit_tables(session)

        # If all the scripts succeeded, we can delete the audit tables.
        if not session.any_error_has_occurred and not dry_run:
            delete_audit_tables(session)

        # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
        session.end()
        duration_str = str(session.ended_at - session.started_at).split(".")[0]  # type: ignore[operator]
        emoji = "✅" if not session.any_error_has_occurred else "❌"
        log.info(f"{emoji} Finished, took {duration_str}, cost ${session.total_billed_dollars:.2f}")


def run_scripts(dag: DAGOfScripts, session: Session):
    table_refs_to_run = determine_table_refs_to_run(
        selected_table_refs=session.selected_table_refs,
        existing_audit_tables=session.existing_audit_tables,
        dag=dag,
        base_dataset=session.base_dataset,
    )
    log.info("🔵 Creating audit tables")
    dag.prepare()
    while dag.is_active():
        # If we're in early end mode, we need to check if any script errored, in which case we
        # have to stop everything.
        if session.any_error_has_occurred:
            log.error("✋ Early ending because an error occurred")
            break

        # Start available jobs
        for script_to_run in dag.iter_scripts(table_refs_to_run):
            # Before executing a script, we need to contextualize it. We have to edit its
            # dependencies, add incremental logic, and set the write context.
            script_to_run = session.add_context_to_script(script_to_run)
            future = session.executor.submit(session.run_script, script_to_run)
            session.run_script_futures[future] = script_to_run

        # Check for scripts that have finished
        done, _ = concurrent.futures.wait(
            session.run_script_futures, return_when=concurrent.futures.FIRST_COMPLETED
        )
        for future in done:
            script_done = session.run_script_futures[future]
            if exception := future.exception():
                log.error(f"Failed running {script_done.table_ref}\n{exception}")
            table_ref = session.remove_write_context_from_table_ref(script_done.table_ref)
            session.run_script_futures_complete[future] = session.run_script_futures.pop(future)
            dag.done(table_ref)


def promote_audit_tables(session: Session):
    log.info("🟢 Promoting audit tables")
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
            log.error(f"Promotion failed\n{exception}")


def delete_audit_tables(session: Session):
    table_refs_to_delete = set(session.existing_audit_tables) | {
        session.add_write_context_to_table_ref(table_ref)
        for table_ref in session.selected_table_refs
    }
    if table_refs_to_delete:
        log.info("🧹 Deleting audit tables")
        delete_table_refs(
            table_refs=table_refs_to_delete,
            database_client=session.database_client,
            executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
            verbose=False,
        )
        session.existing_audit_tables = {}


def determine_table_refs_to_run(
    selected_table_refs: set[TableRef],
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

    """

    normalized_existing_audit_tables = {
        table_ref.remove_audit_suffix().replace_dataset(base_dataset): stats
        for table_ref, stats in existing_audit_tables.items()
    }
    table_refs_to_run = selected_table_refs.copy()
    table_refs_to_run -= set(normalized_existing_audit_tables)

    for table_ref in selected_table_refs & set(normalized_existing_audit_tables):
        script = dag.scripts[table_ref]
        if script.updated_at > normalized_existing_audit_tables[table_ref].updated_at:
            log.info(f"{table_ref} modified, re-running it")
            table_refs_to_run.add(table_ref)
            table_refs_to_run |= set(dag.iter_descendants(table_ref)) & selected_table_refs

    return table_refs_to_run
