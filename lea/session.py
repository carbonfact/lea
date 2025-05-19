from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import re
import threading
import time
from collections.abc import Callable

import lea
from lea.databases import DatabaseClient, TableStats
from lea.field import FieldTag
from lea.job import Job, JobStatus
from lea.scripts import Script
from lea.table_ref import TableRef


class Session:
    def __init__(
        self,
        database_client: DatabaseClient,
        base_dataset: str,
        write_dataset: str,
        scripts: dict[TableRef, Script],
        selected_table_refs: set[TableRef],
        unselected_table_refs: set[TableRef],
        existing_tables: dict[TableRef, TableStats],
        existing_audit_tables: dict[TableRef, TableStats],
        incremental_field_name=None,
        incremental_field_values=None,
    ):
        self.database_client = database_client
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs
        self.unselected_table_refs = unselected_table_refs
        self.existing_tables = existing_tables
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
                table_ref.replace_dataset(self.write_dataset).remove_audit_suffix()
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
            # We don't modify the project if is has been deliberately set
            if dependency.project is not None and dependency.project != script.table_ref.project:
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
        lea.log.info(msg)

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
                    lea.log.info(f"{job.status} {job.table_ref} after {duration_str}")
                    checked_at = now
                time.sleep(delay)
                continue

            # Case 1: the job raised an exception
            if (exception := job.database_job.exception) is not None:
                job.status = JobStatus.ERRORED
                lea.log.error(f"{job.status} {job.table_ref}\n{exception}")

            # Case 2: the job succeeded, but it's a test and there are negative cases
            elif job.is_test and not (dataframe := job.database_job.result).empty:
                job.status = JobStatus.ERRORED
                lea.log.error(f"{job.status} {job.table_ref}\n{dataframe.head()}")

            # Case 3: the job succeeded!
            else:
                job.status = JobStatus.SUCCESS
                msg = f"{job.status} {job.table_ref}"
                job.ended_at = dt.datetime.now()
                # Depending on the warehouse in use, jobs may have a conclude() method, for example
                # for recording job statistics.
                job.database_job.conclude()
                duration_str = str(job.ended_at - job.started_at).split(".")[0]
                if job.ended_at - job.started_at >= dt.timedelta(seconds=1):
                    msg += f", took {duration_str}"
                if job.database_job.billed_dollars is not None:
                    msg += f", cost ${job.database_job.billed_dollars:.2f}"
                if not job.is_test:
                    if (stats := job.database_job.statistics) is not None:
                        msg += f", contains {stats.n_rows:,d} rows"
                        if stats.n_bytes is not None:
                            msg += f", weighs {format_bytes(stats.n_bytes)}"
                if job.database_job.metadata:
                    msg += f" ({', '.join(job.database_job.metadata)})"
                lea.log.info(msg)

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
        lea.log.info(f"{job.status} {job.table_ref}" + (" (incremental)" if is_incremental else ""))

        self.monitor_job(job)

    def end(self):
        lea.log.info("😴 Ending session")
        self.stop_event.set()
        for job in self.jobs:
            if job.status == JobStatus.RUNNING:
                job.database_job.stop()
                job.status = JobStatus.STOPPED
                lea.log.info(f"{job.status} {job.table_ref}")
        self.executor.shutdown()
        self.ended_at = dt.datetime.now()

    @property
    def any_error_has_occurred(self) -> bool:
        return any(job.status == JobStatus.ERRORED for job in self.jobs) or any(
            future.exception() is not None for future in self.run_script_futures_complete
        )

    @property
    def total_billed_dollars(self) -> float:
        return sum(
            job.database_job.billed_dollars
            for job in self.jobs
            if job.database_job.billed_dollars is not None
        )


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
