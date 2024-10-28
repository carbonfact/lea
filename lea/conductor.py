from __future__ import annotations

from collections.abc import Callable
import concurrent.futures
import dataclasses
import datetime as dt
import enum
import logging
import re
import rich.logging
import sys
import time
import threading

from lea.scripts import Script
from lea.databases import DatabaseClient, DatabaseJob
from lea.field import FieldTag
from lea.table_ref import TableRef


log = logging.getLogger(__name__)
log.setLevel("INFO")
log_handler = rich.logging.RichHandler()
log.addHandler(log_handler)



class ScriptJobStatus(enum.Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERRORED = "ERRORED"
    SKIPPED = "SKIPPED"
    STOPPED = "STOPPED"

    def __str__(self):
        return self.value


@dataclasses.dataclass
class ScriptJob:
    script: Script
    database_job: DatabaseJob
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime | None = None
    status: ScriptJobStatus = ScriptJobStatus.RUNNING

    def __hash__(self):
        return hash(self.script.table_ref)


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
    return f"{size:.2f}{units[n]}"


class Session:

    def __init__(
        self,
        database_client: DatabaseClient,
        base_dataset: str,
        write_dataset: str,
        scripts: dict[TableRef, Script],
        selected_table_refs: set[TableRef],
        incremental_field_name=None,
        incremental_field_values=None
    ):
        self.database_client = database_client
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs

        self.incremental_field_name = incremental_field_name
        self.incremental_field_values = incremental_field_values
        self.jobs: list[ScriptJob] = []
        self.started_at = dt.datetime.now()
        self.ended_at: dt.datetime | None = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=None)
        self.start_script_futures: dict = {}
        self.monitor_script_job_futures: dict = {}
        self.promote_futures: dict = {}
        self.stop_event = threading.Event()

        if self.incremental_field_name is not None:
            self.filterable_table_refs = (
                {
                    table_ref
                    for table_ref in scripts
                    if any(field.name == incremental_field_name for field in scripts[table_ref].fields)
                } | {
                    self.add_write_context_to_table_ref(table_ref)
                    for table_ref in selected_table_refs
                    if any(field.name == incremental_field_name for field in scripts[table_ref].fields)
                }
            )
            self.incremental_table_refs = {
                table_ref
                for table_ref in selected_table_refs
                if any(field.name == incremental_field_name and FieldTag.INCREMENTAL in field.tags for field in scripts[table_ref].fields)
            } | {
                self.add_write_context_to_table_ref(table_ref)
                for table_ref in scripts
                if any(field.name == incremental_field_name and FieldTag.INCREMENTAL in field.tags for field in scripts[table_ref].fields)
            }
        else:
            self.filterable_table_refs = set()
            self.incremental_table_refs = set()

    def add_write_context_to_table_ref(self, table_ref: TableRef) -> TableRef:
        return dataclasses.replace(
            table_ref,
            dataset=self.write_dataset,
            name=f"{table_ref.name}___audit"
        )

    def remove_write_context(self, table_ref: TableRef) -> TableRef:
        return self.remove_audit_suffix(dataclasses.replace(
            table_ref,
            dataset=self.base_dataset
        ))

    def remove_audit_suffix(self, table_ref: TableRef) -> TableRef:
        return dataclasses.replace(
            table_ref,
            name=re.sub(r"___audit$", "", table_ref.name)
        )

    def add_context(self, script: Script) -> Script:
        script = replace_script_dependencies(
            script=script,
            table_refs_to_replace=self.selected_table_refs,
            replace_func=self.add_write_context_to_table_ref
        )
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
                    dependencies_to_filter=self.filterable_table_refs - self.incremental_table_refs
                )
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
                        self.remove_write_context(table_ref): table_ref
                        for table_ref in self.incremental_table_refs
                    }
                )
            )

        script = script.replace_table_ref(self.add_write_context_to_table_ref(script.table_ref))
        return script

    def start_script(self, script: Script):
        # If the script is a test, we don't materialize it, we just query it. A test fails if it
        # returns any rows.
        if script.is_test:
            database_job = self.database_client.query_script(script=script)
        # If the script is not a test, it's a regular table, so we materialize it. Instead of
        # directly materializing it to the destination table, we materialize it to a side-table
        # which we call an "audit" table. Once all the scripts have run successfully, we will
        # promote the audit tables to the destination tables. This is the WAP pattern.
        else:
            database_job = self.database_client.materialize_script(
                script=script
            )

        job = ScriptJob(script=script, database_job=database_job)
        self.jobs.append(job)
        log.info(f"{job.status} {script.table_ref}")

        future = self.executor.submit(self.monitor_script_job, job)
        self.monitor_script_job_futures[job] = future
        del self.start_script_futures[script.table_ref]

    def monitor_script_job(self, job: ScriptJob):

        # We're going to do expontential backoff. This is because we don't want to overload
        # whatever API is used to check whether a database job is over or not. We're going to
        # check every second, then every two seconds, then every four seconds, etc. until we
        # reach a maximum delay of 10 seconds.
        base_delay = 1
        max_delay = 10
        retries = 0

        while not self.stop_event.is_set():
            if not job.database_job.is_done:
                delay = min(max_delay, base_delay * (2 ** retries))
                retries += 1
                time.sleep(delay)
                continue

            try:
                job.ended_at = dt.datetime.now()

                # Case 1: the job raised an exception
                if (exception := job.database_job.exception) is not None:
                    job.status = ScriptJobStatus.ERRORED
                    log.error(f"{job.status} {job.script.table_ref}\n{exception}")

                # Case 2: the job succeeded, but it's a test and there are negative cases
                elif job.script.is_test and not (dataframe := job.database_job.result).empty:
                    job.status = ScriptJobStatus.ERRORED
                    log.error(f"{job.status} {job.script.table_ref}\n{dataframe.head()}")

                # Case 3: the job succeeded!
                else:
                    job.status = ScriptJobStatus.SUCCESS
                    msg = f"{job.status} {job.script.table_ref}"
                    duration_str = str(job.ended_at - job.started_at).split('.')[0]
                    msg += f", took {duration_str}, billed ${job.database_job.billed_dollars:.2f}"
                    if not job.script.is_test:
                        if (n_rows := job.database_job.n_rows_in_destination) is not None:
                            msg += f", contains {n_rows:,d} rows"
                        if (n_bytes := job.database_job.n_bytes_in_destination) is not None:
                            msg += f", weighs {format_bytes(n_bytes)}"
                    log.info(msg)

            except Exception as e:
                job.status = ScriptJobStatus.ERRORED
                log.error(f"{job.status} {job.script.table_ref}\n{e}")

            return job

    def promote(self, script: Script):
        from_table_ref = script.table_ref
        script = dataclasses.replace(
            script,
            table_ref=self.remove_write_context(script.table_ref)
        )

        if self.incremental_field_name is not None and script.table_ref in self.incremental_table_refs:
            database_job = self.database_client.delete_and_insert(
                from_table_ref=from_table_ref,
                to_table_ref=script.table_ref,
                on=self.incremental_field_name
            )
        else:
            database_job = self.database_client.clone_table(
                from_table_ref=from_table_ref,
                to_table_ref=script.table_ref
            )

        job = ScriptJob(script=script, database_job=database_job)
        self.jobs.append(job)
        log.info(f"{job.status} {script.table_ref}")

        self.monitor_script_job(job)

    def end(self):
        log.info("Ending session")
        self.stop_event.set()
        for job in self.jobs:
            if job.status == ScriptJobStatus.RUNNING:
                log.info(f"Stopping {job.script.table_ref}")
                job.database_job.stop()
                job.status = ScriptJobStatus.STOPPED
        self.executor.shutdown()
        self.ended_at = dt.datetime.now()

    @property
    def any_job_is_running(self) -> bool:
        return any(job.status == ScriptJobStatus.RUNNING for job in self.jobs)

    @property
    def any_job_has_errored(self) -> bool:
        return any(job.status == ScriptJobStatus.ERRORED for job in self.jobs)

    @property
    def total_billed_dollars(self) -> float:
        return sum(job.database_job.billed_dollars for job in self.jobs)


def replace_script_dependencies(
    script: Script,
    table_refs_to_replace: set[TableRef],
    replace_func: Callable[[TableRef], TableRef]
) -> Script:
    """

    It's often necessary to edit the dependencies of a script. For example, we might want
    to change the dataset of a dependency. Or we might want to append a suffix a table name
    when we're doing a write/audit/publish operation.

    """
    code = script.code
    for dependency_to_edit in script.dependencies & table_refs_to_replace:
        dependency_to_edit_str = script.sql_dialect.format_table_ref(dependency_to_edit)
        new_dependency = replace_func(dependency_to_edit)
        new_dependency_str = script.sql_dialect.format_table_ref(new_dependency)
        code = re.sub(rf"\b{dependency_to_edit_str}\b", new_dependency_str, code)
    return dataclasses.replace(script, code=code)


def main():

    import pathlib
    from lea.dialects import BigQueryDialect
    from lea.dag import DAGOfScripts
    from lea.databases import BigQueryClient

    dag = DAGOfScripts.from_directory(
        dataset_dir=pathlib.Path("kaya"),
        sql_dialect=BigQueryDialect()
    )
    #query = ['core.accounts', 'tests.customers_have_arr', 'core.dates', 'core.carbonverses']
    query = ['core.accounts', 'core.accounts_dup', 'tests.customers_have_arr', 'core.dates']
    #query = ['core.accounts']

    dry_run = False
    early_end = True
    selected_table_refs = dag.select(*query)

    if not selected_table_refs:
        log.error("Nothing found for queries: " + ", ".join(query))
        return sys.exit(1)

    database_client = BigQueryClient(
        credentials=None,
        location="EU",
        write_project_id="carbonfact-gsheet",
        compute_project_id="carbonfact-gsheet",
        dry_run=dry_run
    )
    dataset = "kaya"
    username = "max"
    write_dataset = f"{dataset}_{username}"
    database_client.create_dataset(write_dataset)

    session = Session(
        database_client=database_client,
        base_dataset=dataset,
        write_dataset=write_dataset,
        scripts=dag.scripts,
        selected_table_refs=selected_table_refs,
        incremental_field_name='account_slug',
        incremental_field_values=['demo-account']
    )


    # Loop over table references in topological order
    dag.prepare()
    while dag.is_active():

        # If we're in early end mode, we need to check if any script errored, in which case we
        # have to stop everything.
        if early_end and session.any_job_has_errored:
            log.error("Early ending because a job errored")
            break

        # Start available jobs
        for script in dag.iter_scripts(selected_table_refs):

            # Before executing a script, we need to contextualize it. We have to edit its
            # dependencies, add incremental logic, and set the write context.
            script = session.add_context(script)

            future = session.executor.submit(session.start_script, script)
            session.start_script_futures[script.table_ref] = future

        # Check for scripts that have finished
        for job, future in list(session.monitor_script_job_futures.items()):
            if future.done():
                table_ref = session.remove_write_context(job.script.table_ref)
                dag.done(table_ref)
                del session.monitor_script_job_futures[job]

    # At this point, the scripts have been materialized into side-tables which we call "audit"
    # tables. We can now take care of promoting the audit tables to production.
    if not session.any_job_has_errored and not dry_run:
        log.info("Promoting tables")

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
        for script in [job.script for job in session.jobs if not job.script.is_test]:
            future = session.executor.submit(session.promote, script)
            session.promote_futures[script.table_ref] = future

        # Wait for all promotion jobs to finish
        for future in concurrent.futures.as_completed(session.promote_futures.values()):
            ...

    # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
    session.end()
    duration_str = str(session.ended_at - session.started_at).split('.')[0]
    log.info(f"Finished, took {duration_str}, billed ${session.total_billed_dollars:.2f}")

    # # for job in session.jobs:
    # #     print(job.script.table_ref)
    # #     print('-' * 80)
    # #     print(job.database_job.query_job.query)
    # #     print('=' * 80)

    if session.any_job_has_errored:
        return sys.exit(1)


if __name__ == "__main__":
    main()
