from __future__ import annotations

import dataclasses
import datetime as dt
import enum

from lea.databases import DatabaseJob
from lea.table_ref import TableRef


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
