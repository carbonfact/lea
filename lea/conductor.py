import concurrent.futures
import dataclasses
import datetime as dt
import functools
import enum
import logging
import rich.logging
import sys
from typing import Iterator

import pandas as pd

from lea.scripts import Script
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

    def run(self, script: Script, client: Client, write_dataset: str, is_dry_run: bool) -> Job:
        if script.is_test:
            func = functools.partial(client.query_script, script=script, is_dry_run=is_dry_run)
        else:
            func = functools.partial(client.materialize_script, script=script.replace_dataset(write_dataset), is_dry_run=is_dry_run)

        job = Job(script=script, future=self.executor.submit(func))
        self.jobs.append(job)
        log.info(f"{job.status} {script.table_ref}")
        return job

    def shutdown(self):
        for job in self.jobs:
            if job.status == JobStatus.RUNNING:
                job.future.cancel()
                job.status = JobStatus.STOPPED
                job.ended_at = dt.datetime.now()
                logging.info(f"{job.status} {job.script.table_ref}")

    @property
    def jobs_in_progress(self) -> Iterator[Job]:
        return (job for job in self.jobs if job.status == JobStatus.RUNNING)

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

    client = BigQueryClient(
        credentials=None,
        location="EU",
        write_project_id="carbonfact-gsheet",
        compute_project_id="carbonfact-gsheet",
    )
    write_dataset = "kaya_max"
    client.create_dataset(write_dataset)
    dry = False

    session = Session()

    # Loop over table references in topological order
    dag.prepare()
    while dag.is_active():

        # Start available jobs
        for script in dag.iter_scripts(*query):
            session.run(script=script, client=client, write_dataset=write_dataset, is_dry_run=dry)

        # Check running jobs and update their status
        for job in session.jobs_in_progress:
            if not job.is_done:
                continue
            job.ended_at = dt.datetime.now()
            dag.done(job.script.table_ref)
            if job.exception:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {job.script.table_ref}\n{job.exception}")
            elif job.script.is_test and not (dataframe := job.result.output_dataframe).empty:
                job.status = JobStatus.ERRORED
                log.error(f"{job.status} {job.script.table_ref}\n{dataframe.head()}")
            else:
                job.status = JobStatus.SUCCESS
                log.info(f"{job.status} {job.script.table_ref} for ${job.result.billed_dollars:.2f}")

    # Handle with what happens when the session is over
    if session.all_jobs_are_success:
        session.ended_at = dt.datetime.now()
        duration_str = str(session.ended_at - session.started_at).split('.')[0]
        log.info(f"Finished in {duration_str} for ${session.total_billed_dollars:.2f}")
    else:
        return sys.exit(1)


if __name__ == "__main__":
    main()
