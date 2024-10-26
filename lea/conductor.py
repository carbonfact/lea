import dataclasses
import datetime as dt
import enum
import logging
import re
import rich.logging
import sys
from typing import Iterator

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


@dataclasses.dataclass
class Session:
    database_client: DatabaseClient
    jobs: list[ScriptJob] = dataclasses.field(default_factory=list)
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime | None = None

    def run(self, script: Script):
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

    def promote(self, script: Script, incremental_field_name: str | None = None):
        from_table_ref = script.table_ref
        script = script.replace_table_ref(from_table_ref.remove_audit_suffix())

        if incremental_field_name is not None:
            database_job = self.database_client.delete_and_insert(
                from_table_ref=from_table_ref,
                to_table_ref=script.table_ref,
                on=incremental_field_name
            )
        else:
            database_job = self.database_client.clone_table(
                from_table_ref=from_table_ref,
                to_table_ref=script.table_ref
            )
        job = ScriptJob(script=script, database_job=database_job)
        self.jobs.append(job)
        log.info(f"{job.status} {script.table_ref}")

    def check_jobs_running(self) -> Iterator[ScriptJob]:

        for job in (job for job in self.jobs if job.status == ScriptJobStatus.RUNNING):
            if not job.database_job.is_done:
                continue

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
                if not job.script.is_test and (n_rows := job.database_job.n_rows_in_destination) is not None:
                    msg += f", table now has {n_rows:,d} rows"
                log.info(msg)

            yield job

    def end(self):
        log.info("Ending session")
        for job in self.jobs:
            if job.status == ScriptJobStatus.RUNNING:
                log.info(f"Stopping {job.script.table_ref}")
                job.database_job.stop()
                job.status = ScriptJobStatus.STOPPED
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


class ScriptManager:

    def __init__(
        self,
        scripts: dict[TableRef, Script],
        selected_table_refs: set[TableRef],
        base_dataset: str,
        write_dataset: str,
        incremental_field_name: str | None = None,
        incremental_field_values: set[str] | None = None,
    ):
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.incremental_field_name = incremental_field_name
        self.incremental_field_values = incremental_field_values

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
            if incremental_field_name
            else set()
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

    def add_context(self, script: Script) -> Script:
        script = self.edit_dependencies(script)
        script = self.add_incremental_context(script)
        script = script.replace_table_ref(self.add_write_context_to_table_ref(script.table_ref))
        return script

    def add_write_context_to_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.add_audit_suffix()
        return dataclasses.replace(
            table_ref,
            dataset=self.write_dataset
        )

    def remove_write_context(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.remove_audit_suffix()
        return dataclasses.replace(
            table_ref,
            dataset=self.base_dataset,
            name=re.sub(r"__audit$", "", table_ref.name)
        )

    def edit_dependencies(self, script: Script) -> Script:
        """

        It's often necessary to edit the dependencies of a script. For example, we might want
        to change the dataset of a dependency. Or we might want to append a suffix a table name
        when we're doing a write/audit/publish operation.

        """
        code = script.code
        # TODO: could be done faster with Aho–Corasick algorithm
        # Maybe try out https://github.com/vi3k6i5/flashtext
        for dependency_to_edit in script.dependencies & self.selected_table_refs:
            dependency_to_edit_str = script.sql_dialect.format_table_ref(dependency_to_edit)
            new_dependency = self.add_write_context_to_table_ref(dependency_to_edit)
            new_dependency_str = script.sql_dialect.format_table_ref(new_dependency)
            code = re.sub(rf"\b{dependency_to_edit_str}\b", new_dependency_str, code)
        return dataclasses.replace(script, code=code)

    def add_incremental_context(self, script: Script) -> Script:
        """

        Some scripts have the ability to be run incrementally. This is useful when we want to
        run a script only for a subset of the data. For example, we might want to run a script
        only for a specific customer. This function modifies the script to only run for the
        specified subset.

        The way this works is to replace each dependency with a subquery that filters the data
        based on the field name and the field values subset. Furthermore, the script is modified
        to filter the data based on the field name and the field values subset. The latter
        guarantees that the output will only contain the specified subset. The former guarantees
        the script isn't processing unnecessary data.

        """
        if script.table_ref in self.incremental_table_refs:
            code_with_incremental_logic = script.sql_dialect.make_incremental(
                code=script.code,
                field_name=self.incremental_field_name,
                field_values=self.incremental_field_values,
                dependencies_to_filter=self.filterable_table_refs - self.incremental_table_refs
            )
            script = dataclasses.replace(script, code=code_with_incremental_logic)

        else:
            code = script.code
            for dependency in self.incremental_table_refs:
                dependency_str = script.sql_dialect.format_table_ref(dependency)
                code = code.replace(
                    dependency_str,
                    f"""
                    (
                        SELECT * FROM {dependency_str}
                        UNION ALL
                        SELECT * FROM {script.sql_dialect.format_table_ref(dependency.remove_audit_suffix())}
                        WHERE {self.incremental_field_name} NOT IN ({", ".join(f"'{value}'" for value in self.incremental_field_values)})
                    )
                    """
                )
            script = dataclasses.replace(script, code=code)

        return script


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

    script_manager = ScriptManager(
        scripts=dag.scripts,
        selected_table_refs=selected_table_refs,
        write_dataset=write_dataset,
        base_dataset=dataset,
        incremental_field_name="account_slug",
        incremental_field_values={"demo-account"}
    )

    session = Session(database_client=database_client)

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
            script = script_manager.add_context(script)

            session.run(script=script)

        # Check running jobs and update their status
        for job in session.check_jobs_running():
            # We have to tell the DAG that a script has finished running in order to unlock the
            # next scripts.
            table_ref = script_manager.remove_write_context(job.script.table_ref)
            dag.done(table_ref)

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
            session.promote(
                script=script,
                incremental_field_name=(
                    script_manager.incremental_field_name
                    if script.table_ref in script_manager.incremental_table_refs
                    else None
                )
            )

        # Wait for all promotion jobs to finish
        while session.any_job_is_running:
            for job in session.check_jobs_running():
                pass

    # Regardless of whether all the jobs succeeded or not, we want to summarize the session.
    session.end()
    duration_str = str(session.ended_at - session.started_at).split('.')[0]
    log.info(f"Finished, took {duration_str}, billed ${session.total_billed_dollars:.2f}")

    # for job in session.jobs:
    #     print(job.script.table_ref)
    #     print('-' * 80)
    #     print(job.database_job.query_job.query)
    #     print('=' * 80)

    if session.any_job_has_errored:
        return sys.exit(1)


if __name__ == "__main__":
    main()
