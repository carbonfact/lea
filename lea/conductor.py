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

import rich.logging

from lea import databases
from lea.dag import DAGOfScripts
from lea.databases import DatabaseClient, DatabaseJob
from lea.dialects import BigQueryDialect
from lea.field import FieldTag
from lea.scripts import Script
from lea.table_ref import AUDIT_TABLE_SUFFIX, TableRef

log = logging.getLogger(__name__)
log.setLevel("INFO")
log_handler = rich.logging.RichHandler()
log.addHandler(log_handler)


class JobStatus(enum.Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERRORED = "ERRORED"
    SKIPPED = "SKIPPED"
    STOPPED = "STOPPED"

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
        materialized_table_refs: set[TableRef],
        incremental_field_name=None,
        incremental_field_values=None,
    ):
        self.database_client = database_client
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs
        self.materialized_table_refs = materialized_table_refs
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
                table_ref
                for table_ref in scripts
                if any(field.name == incremental_field_name for field in scripts[table_ref].fields)
            }
            self.incremental_table_refs = {
                table_ref
                for table_ref in selected_table_refs | materialized_table_refs
                if any(
                    field.name == incremental_field_name and FieldTag.INCREMENTAL in field.tags
                    for field in scripts[table_ref].fields
                )
            }
        else:
            self.filterable_table_refs = set()
            self.incremental_table_refs = set()

    @property
    def table_refs_to_run(self) -> set[TableRef]:
        return self.selected_table_refs - self.materialized_table_refs

    def add_write_context_to_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.write_dataset)
        table_ref = table_ref.add_audit_suffix()
        return table_ref

    def remove_write_context_from_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.base_dataset)
        table_ref = table_ref.remove_audit_suffix()
        return table_ref

    def add_context_to_script(self, script: Script) -> Script:
        # If a script is marked as incremental, it implies that it can be run incrementally. This
        # means that we have to filter the script's dependencies, as well as filter the output.
        # This logic is implemented by the script's SQL dialect.
        if script.table_ref in self.incremental_table_refs:
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

        def add_context_to_dependency(dependency: TableRef) -> TableRef:
            if (
                dependency in self.selected_table_refs | self.materialized_table_refs
                and dependency in self.scripts
            ):
                dependency = dependency.add_audit_suffix()
            dependency = dependency.replace_dataset(self.write_dataset)
            return dependency

        script = replace_script_dependencies(script=script, replace_func=add_context_to_dependency)

        # If the script is not incremental, we're not out of the woods! All scripts are
        # materialized into side-tables which we call "audit" tables. This is the WAP pattern.
        # Therefore, if a script is not incremental, but it depends on an incremental script, we
        # have to modify the script to use both the incremental and non-incremental versions of
        # the dependency. This is handled by the script's SQL dialect.
        if script.table_ref not in self.incremental_table_refs and self.incremental_table_refs:
            script = dataclasses.replace(
                script,
                code=script.sql_dialect.handle_incremental_dependencies(
                    code=script.code,
                    incremental_field_name=self.incremental_field_name,
                    incremental_field_values=self.incremental_field_values,
                    incremental_dependencies={
                        table_ref.replace_dataset(
                            self.write_dataset
                        ): self.add_write_context_to_table_ref(table_ref)
                        for table_ref in self.incremental_table_refs
                    },
                ),
            )

        if not script.is_test:
            return script.replace_table_ref(self.add_write_context_to_table_ref(script.table_ref))
        return script.replace_table_ref(script.table_ref.replace_dataset(self.write_dataset))

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
        if script.table_ref in self.incremental_table_refs:
            msg += " (incremental)"
        log.info(msg)

        self.monitor_job(job)

    def monitor_job(self, job: Job):
        # We're going to do expontential backoff. This is because we don't want to overload
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
                    log.info(f"{job.table_ref} still running after {duration_str}")
                    checked_at = now
                time.sleep(delay)
                continue

            try:
                job.ended_at = dt.datetime.now()

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
                    duration_str = str(job.ended_at - job.started_at).split(".")[0]
                    msg += f", took {duration_str}, billed ${job.database_job.billed_dollars:.2f}"
                    if not job.is_test:
                        if (stats := job.database_job.statistics) is not None:
                            msg += f", contains {stats.n_rows:,d} rows"
                            msg += f", weighs {format_bytes(stats.n_bytes)}"
                    log.info(msg)

            except Exception as e:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {job.table_ref}\n{e}")

            return

    def promote_audit_table(self, table_ref: TableRef):
        from_table_ref = table_ref
        to_table_ref = table_ref.remove_audit_suffix()

        if (
            self.incremental_field_name is not None
            and from_table_ref in self.incremental_table_refs
        ):
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
        log.info(f"{job.status} {job.table_ref}")

        self.monitor_job(job)

    def end(self):
        log.info("😴 Ending session")
        self.stop_event.set()
        for job in self.jobs:
            if job.status == JobStatus.RUNNING:
                log.info(f"Stopping {job.table_ref}")
                job.database_job.stop()
                job.status = JobStatus.STOPPED
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
        dependency_to_edit_str = script.sql_dialect.format_table_ref(dependency_to_edit)
        new_dependency = replace_func(dependency_to_edit)
        new_dependency_str = script.sql_dialect.format_table_ref(new_dependency)
        code = re.sub(rf"\b{dependency_to_edit_str}\b", new_dependency_str, code)

        # We also have to handle the case where the table is referenced to access a field.
        # TODO: refactor this with the above
        dependency_to_edit_without_dataset = dataclasses.replace(dependency_to_edit, dataset="")
        dependency_to_edit_without_dataset_str = script.sql_dialect.format_table_ref(
            dependency_to_edit_without_dataset
        )
        new_dependency_without_dataset = dataclasses.replace(new_dependency, dataset="")
        new_dependency_without_dataset_str = script.sql_dialect.format_table_ref(
            new_dependency_without_dataset
        )
        code = re.sub(
            rf"\b{dependency_to_edit_without_dataset_str}\b",
            new_dependency_without_dataset_str,
            code,
        )

    return dataclasses.replace(script, code=code)


def delete_audit_tables(
    table_refs: set[TableRef],
    database_client: DatabaseClient,
    executor: concurrent.futures.ThreadPoolExecutor,
    verbose: bool,
):
    futures: dict[concurrent.futures.Future, TableRef] = {}
    for table_ref in table_refs:
        table_ref = table_ref.add_audit_suffix()
        future = executor.submit(database_client.delete_table, table_ref)
        futures[future] = table_ref

    for future in concurrent.futures.as_completed(futures):
        if future.exception() is not None:
            log.error(future.exception())
            continue
        if verbose:
            log.info(f"Deleted {futures[future]}")


class Conductor:
    def __init__(self, scripts_dir: str, dataset_name: str | None = None):
        self.scripts_dir = pathlib.Path(scripts_dir)
        if not self.scripts_dir.is_dir():
            raise ValueError(f"Directory {self.scripts_dir} not found")
        dataset_name = dataset_name or os.environ.get("LEA_BQ_DATASET_NAME")
        if dataset_name is None:
            raise ValueError("Dataset name could not be inferred")
        self.dataset_name = dataset_name
        self.dag = DAGOfScripts.from_directory(
            scripts_dir=self.scripts_dir,
            sql_dialect=BigQueryDialect(),
            dataset_name=self.dataset_name,
        )

    def make_client(self, dry_run: bool = False):
        warehouse = os.environ["LEA_WAREHOUSE"]

        if warehouse.lower() == "bigquery":
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
            )

        raise ValueError(f"Unsupported warehouse {warehouse!r}")

    def name_user_dataset(self) -> str:
        username = os.environ.get("LEA_USERNAME", getpass.getuser())
        return f"{self.dataset_name}_{username}"

    def list_materialized_audit_table_refs(
        self, database_client: DatabaseClient, dataset: str
    ) -> set[TableRef]:
        existing_tables = database_client.list_tables(dataset)
        existing_audit_tables = {
            table_ref: stats
            for table_ref, stats in existing_tables.items()
            if table_ref.name.endswith(AUDIT_TABLE_SUFFIX)
        }
        return {table_ref.remove_audit_suffix().replace_dataset(self.dataset_name) for table_ref in existing_audit_tables}

    def run(
        self,
        *query: str,
        production: bool = False,
        dry_run: bool = False,
        keep_going: bool = False,
        fresh: bool = False,
        incremental_field_name: str | None = None,
        incremental_field_values: list[str] | None = None,
        print_mode: bool = False,
    ):
        # We need a database client to run scripts
        database_client = self.make_client(dry_run=dry_run)

        # We need to select the scripts we want to run. We do this by querying the DAG.
        selected_table_refs = self.dag.select(*query)
        if not selected_table_refs:
            log.error("Nothing found for query: " + ", ".join(query))
            return sys.exit(1)

        # We need a dataset to materialize the scripts. If we're in production mode, we use the
        # base dataset. If we're in user mode, we use a dataset named after the user.
        write_dataset = self.dataset_name if production else self.name_user_dataset()
        database_client.create_dataset(write_dataset)

        # When the scripts run, they are materialized into side-tables which we call "audit"
        # tables. When a run stops because of an error, the audit tables are left behind. If we
        # want to start fresh, we have to delete the audit tables. If not, the materialized tables
        # can be skipped.
        materialized_table_refs = self.list_materialized_audit_table_refs(
            database_client, write_dataset
        )
        if fresh and materialized_table_refs:
            log.info("🧹 Starting fresh, deleting audit tables")
            delete_audit_tables(
                table_refs={
                    table_ref.replace_dataset(write_dataset)
                    for table_ref in materialized_table_refs
                },
                database_client=database_client,
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
                verbose=True,
            )
            materialized_table_refs = set()

        session = Session(
            database_client=database_client,
            base_dataset=self.dataset_name,
            write_dataset=write_dataset,
            scripts=self.dag.scripts,
            selected_table_refs=selected_table_refs,
            materialized_table_refs=materialized_table_refs,
            incremental_field_name=incremental_field_name,
            incremental_field_values=incremental_field_values,
        )

        try:
            self.run_session(session, keep_going=keep_going, dry_run=dry_run, print_mode=print_mode)
        except KeyboardInterrupt:
            log.error("🛑 Keyboard interrupt")
            session.end()
            return sys.exit(1)

        # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
        session.end()
        duration_str = str(session.ended_at - session.started_at).split(".")[0]  # type: ignore[operator]
        emoji = "🟢" if not session.any_error_has_occurred else "🔴"
        log.info(
            f"{emoji} Finished, took {duration_str}, billed ${session.total_billed_dollars:.2f}"
        )

        if session.any_error_has_occurred:
            return sys.exit(1)

    def run_session(self, session: Session, keep_going: bool, dry_run: bool, print_mode: bool):

        # In print mode, we just print the scripts. There's no need to run them. We take care of
        # printing them in topological order.
        if print_mode:
            for table_ref in self.dag.static_order():
                if table_ref not in session.selected_table_refs:
                    continue
                script = self.dag.scripts[table_ref]
                script = session.add_context_to_script(script)
                rich.print(script)
            return

        # Loop over table references in topological order
        self.dag.prepare()
        while self.dag.is_active():
            # If we're in early end mode, we need to check if any script errored, in which case we
            # have to stop everything.
            if session.any_error_has_occurred and not keep_going:
                log.error("✋ Early ending because an error occurred")
                break

            # Start available jobs
            for script in self.dag.iter_scripts(session.table_refs_to_run):
                # Before executing a script, we need to contextualize it. We have to edit its
                # dependencies, add incremental logic, and set the write context.
                script = session.add_context_to_script(script)
                future = session.executor.submit(session.run_script, script)
                session.run_script_futures[future] = script

            # Check for scripts that have finished
            done, _ = concurrent.futures.wait(
                session.run_script_futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                if future.exception():
                    log.error(f"Failed running {script.table_ref}\n{future.exception()}")
                table_ref = session.remove_write_context_from_table_ref(script.table_ref)
                self.dag.done(table_ref)
                session.run_script_futures_complete[future] = session.run_script_futures.pop(future)

        # At this point, the scripts have been materialized into side-tables which we call "audit"
        # tables. We can now take care of promoting the audit tables to production.
        if not session.any_error_has_occurred and not dry_run:
            table_refs_to_promote = {
                session.add_write_context_to_table_ref(table_ref)
                for table_ref in session.table_refs_to_run | session.materialized_table_refs
            }
            if table_refs_to_promote:
                log.info("🥇 Promoting audit tables")

                # Ideally, we would like to do this atomatically, but BigQuery does not support DDL
                # statements in a transaction. So we do it concurrently. This isn't ideal, but it's the
                # best we can do for now. There's a very small chance that at least one promotion job will
                # fail.
                # https://hiflylabs.com/blog/2022/11/22/dbt-deployment-best-practices
                # https://calogica.com/sql/bigquery/dbt/2020/05/24/dbt-bigquery-blue-green-wap.html
                # https://calogica.com/assets/wap_dbt_bigquery.pdf
                # Note: it's important for the following loop to be a list comprehension. If we used a
                # generator expression, the loop would be infinite because jobs are being added to
                # session.jobs when session.promote is called.
                for table_ref in table_refs_to_promote:
                    future = session.executor.submit(session.promote_audit_table, table_ref)
                    session.promote_audit_tables_futures[future] = script

                # Wait for all promotion jobs to finish
                for future in concurrent.futures.as_completed(session.promote_audit_tables_futures):
                    if future.exception() is not None:
                        log.error(f"Promotion failed\n{future.exception()}")

        # If all the scripts succeeded, we can delete the audit tables.
        if not session.any_error_has_occurred and not dry_run:
            audit_tables = {
                table_ref.replace_dataset(session.write_dataset)
                for table_ref in (
                    {
                        table_ref
                        for table_ref in session.table_refs_to_run
                        if not self.dag.scripts[table_ref].is_test
                    }
                    | session.materialized_table_refs
                )
            }
            if audit_tables:
                log.info("🧹 Deleting audit tables")
                delete_audit_tables(
                    table_refs=audit_tables,
                    database_client=session.database_client,
                    executor=concurrent.futures.ThreadPoolExecutor(max_workers=None),
                    verbose=True,
                )
