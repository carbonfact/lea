import concurrent.futures
import dataclasses
import datetime as dt
import functools
import enum
import logging
import rich.logging
import sys
from typing import Iterator

from lea.scripts import Script
from lea.table_ref import TableRef
from lea.clients import Client, JobResult


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
    script: Script
    write_table_ref: TableRef | None
    future: concurrent.futures.Future
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime | None = None
    status: JobStatus = JobStatus.RUNNING

    @property
    def is_done(self) -> bool:
        return self.future.done()

    @property
    def exception(self) -> BaseException | None:
        return self.future.exception()

    @property
    def result(self) -> JobResult:
        return self.future.result()


@dataclasses.dataclass
class Session:
    jobs: list[Job] = dataclasses.field(default_factory=list)
    done: bool = False
    executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor()
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime | None = None

    def run(self, script: Script, client: Client, write_dataset: str, is_dry_run: bool):
        write_table_ref = None
        # If the script is a test, we don't materialize it, we just query it. A test fails if it
        # returns any rows.
        if script.is_test:
            func = functools.partial(client.query_script, script=script, is_dry_run=is_dry_run)
        # If the script is not a test, it's a regular table, so we materialize it. Instead of
        # directly materializing it to the destination table, we materialize it to a side-table
        # which we call an "audit" table. Once all the scripts have run successfully, we will
        # promote the audit tables to the destination tables. This is the WAP pattern.
        else:
            write_table_ref = script.table_ref.replace_dataset(write_dataset).add_suffix("audit")
            func = functools.partial(
                client.materialize_script,
                script=dataclasses.replace(script, table_ref=write_table_ref),
                is_dry_run=is_dry_run
            )

        job = Job(script=script, write_table_ref=write_table_ref, future=self.executor.submit(func))
        self.jobs.append(job)
        log.info(f"{job.status} {write_table_ref or script.table_ref}")

    def promote(self, script: Script, client: Client, write_dataset: str):
        to_table_ref = script.table_ref.replace_dataset(write_dataset)
        func = functools.partial(
            client.clone_table,
            from_table_ref=script.table_ref.replace_dataset(write_dataset).add_suffix("audit"),
            to_table_ref=to_table_ref
        )
        job = Job(script=script, write_table_ref=to_table_ref, future=self.executor.submit(func))
        self.jobs.append(job)
        log.info(f"{job.status} {to_table_ref}")

    def check_finished_jobs(self) -> Iterator[Job]:

        for job in self.jobs_running:
            if not job.is_done:
                continue

            job.ended_at = dt.datetime.now()
            table_ref = job.write_table_ref or job.script.table_ref

            # Case 1: the job raised an exception
            if job.exception:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {table_ref}\n{job.exception}")

            # Case 2: the job succeeded, but it's a test and there are negative cases
            elif job.script.is_test and not (dataframe := job.result.output_dataframe).empty:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {table_ref}\n{dataframe.head()}")

            # Case 3: the job succeeded!
            else:
                job.status = JobStatus.SUCCESS
                msg = f"{job.status} {table_ref}"
                msg += f", billed ${job.result.billed_dollars:.2f}"
                if job.result.n_rows_in_destination is not None:
                    msg += f", {job.result.n_rows_in_destination:,d} rows in table"
                log.info(msg)

            yield job


    @property
    def jobs_running(self) -> Iterator[Job]:
        return (job for job in self.jobs if job.status == JobStatus.RUNNING)

    @property
    def any_job_is_running(self) -> bool:
        return any(job.status == JobStatus.RUNNING for job in self.jobs)

    @property
    def all_jobs_are_success(self) -> bool:
        return all(job.status == JobStatus.SUCCESS for job in self.jobs)

    @property
    def total_billed_dollars(self) -> float:
        return sum(job.result.billed_dollars for job in self.jobs if job.is_done)


def main():

    import pathlib
    from lea.dialects import BigQueryDialect
    from lea.dag import DAGOfScripts
    from lea.clients import BigQueryClient

    dag = DAGOfScripts.from_directory(
        dataset_dir=pathlib.Path("kaya"),
        sql_dialect=BigQueryDialect()
    )
    query = ['core.material_taxonomy', 'core.accounts']
    #query = ['core.accounts']
    selected_table_refs = dag.select(*query)

    if not selected_table_refs:
        log.error("Nothing found for queries: " + ", ".join(query))
        return sys.exit(1)

    client = BigQueryClient(
        credentials=None,
        location="EU",
        write_project_id="carbonfact-gsheet",
        compute_project_id="carbonfact-gsheet",
    )
    write_dataset = "kaya_max"
    client.create_dataset(write_dataset)
    dry = False

    # ---

    # TODO: replace this with a proper way to edit the dependencies


    # script = dag[list(selected_table_refs)[0]]
    # for field in script.fields:
    #     print(field, field.tags)

    script = dag[TableRef("kaya", ("core",), "accounts")]
    script = script.make_incremental(field_name="ticker", field_values_subset={"ON", "CAR"})
    #print(script.code)
    dag[script.table_ref] = script

    # ---

    session = Session()

    # Loop over table references in topological order
    dag.prepare()
    while dag.is_active():

        # Start available jobs
        for script in dag.iter_scripts(selected_table_refs):
            session.run(
                script=script,
                client=client,
                write_dataset=write_dataset,
                is_dry_run=dry
            )

        # Check running jobs and update their status
        for job in session.check_finished_jobs():
            # We have to tell the DAG that a script has finished running in order to unlock the
            # next scripts.
            dag.done(job.script.table_ref)

    # At this point, the scripts have been materialized into side-tables which we call "audit"
    # tables. We can now take care of promoting the audit tables to production.
    if session.all_jobs_are_success and not dry:
        log.info("Promoting tables")

        # Ideally, we would like to do this atomatically, but BigQuery does not support DDL
        # statements in a transaction. So we do it concurrently. This isn't ideal, but it's the
        # best we can do for now. There's a very small chance that at least one promotion job will
        # fail.
        # https://hiflylabs.com/blog/2022/11/22/dbt-deployment-best-practices
        # https://calogica.com/sql/bigquery/dbt/2020/05/24/dbt-bigquery-blue-green-wap.html
        # https://calogica.com/assets/wap_dbt_bigquery.pdf
        for script in [job.script for job in session.jobs if not script.is_test]:
            session.promote(
                script=script,
                client=client,
                write_dataset=write_dataset
            )

        # Wait for all promotion jobs to finish
        while session.any_job_is_running:
            for job in session.check_finished_jobs():
                pass

    # Handle with what happens when the session is over
    if session.all_jobs_are_success:
        session.ended_at = dt.datetime.now()
        duration_str = str(session.ended_at - session.started_at).split('.')[0]
        log.info(f"Finished in {duration_str} for ${session.total_billed_dollars:.2f}")
    else:
        return sys.exit(1)


if __name__ == "__main__":
    main()
