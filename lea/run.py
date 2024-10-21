import concurrent.futures
import dataclasses
import datetime as dt
import functools
import enum
import logging
import rich.logging
from typing import Iterator

import pandas as pd

from lea.scripts import Script
from lea.clients import Client


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

    def is_done(self) -> bool:
        return self.future.done()

    @property
    def exception(self) -> BaseException | None:
        if self.future.done() and (exception := self.future.exception()):
            return exception
        return None

    @property
    def billed_dollars(self) -> float | None:
        if result := self.future.result():
            return result.billed_dollars
        return None

    @property
    def dataframe(self) -> pd.DataFrame:
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
        return sum(job.billed_dollars for job in self.jobs if job.billed_dollars)


if __name__ == "__main__":
    import pathlib
    from lea.dialects import BigQueryDialect
    from lea.dag import DAGOfScripts
    from lea.clients import BigQueryClient

    dag = DAGOfScripts.from_directory(
        dataset_dir=pathlib.Path("kaya"),
        sql_dialect=BigQueryDialect()
    )
    client = BigQueryClient(
        credentials=None,
        location="EU",
        write_project_id="carbonfact-gsheet",
        compute_project_id="carbonfact-gsheet",
    )
    write_dataset = "kaya_max"
    query = ['tests/']
    dry = True

    session = Session()

    # Loop over table references in topological order
    dag.prepare()
    while dag.is_active():

        # Start jobs
        for script in dag.iter_scripts(*query):
            session.run(script=script, client=client, write_dataset=write_dataset, is_dry_run=dry)

        # Check jobs
        for job in session.jobs_in_progress:
            if job.is_done():
                job.ended_at = dt.datetime.now()
                dag.done(job.script.table_ref)
                if job.exception:
                    job.status = JobStatus.ERRORED
                    log.error(f"{job.status} {job.script.table_ref}\n{job.exception}")
                elif job.script.is_test and not (dataframe := job.dataframe).empty:
                    job.status = JobStatus.ERRORED
                    log.error(f"{job.status} {job.script.table_ref}\n{dataframe}")
                else:
                    job.status = JobStatus.SUCCESS
                    log.info(f"{job.status} {job.script.table_ref} for ${job.billed_dollars:.2f}")

    if session.all_jobs_are_success:
        session.ended_at = dt.datetime.now()
        log.info(f"Finished in {session.ended_at - session.started_at} for ${session.total_billed_dollars:.2f}")

    # TODO: graceful shutdown
