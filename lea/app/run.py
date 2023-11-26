from __future__ import annotations

import concurrent.futures
import datetime as dt
import functools
import pathlib
import pickle
import time

import rich.console
import rich.live
import rich.syntax

import lea

RUNNING = "[white]RUNNING"
SUCCESS = "[green]SUCCESS"
ERRORED = "[red]ERRORED"
SKIPPED = "[yellow]SKIPPED"
FROZEN = "[cyan]FROZEN"


def _do_nothing(*args, **kwargs):
    """This is a dummy function for dry runs"""


def pretty_print_view(view: lea.views.View, console: rich.console.Console) -> str:
    if isinstance(view, lea.views.SQLView):
        syntax = rich.syntax.Syntax(view.query, "sql")
    elif isinstance(view, lea.views.PythonView):
        syntax = rich.syntax.Syntax(view.path.read_text(), "python")
    else:
        raise NotImplementedError
    console.print(syntax)


def run(
    client: lea.clients.Client,
    views: list[lea.views.View],
    select: list[str],
    freeze_roots: bool,
    print_views: bool,
    dry: bool,
    silent: bool,
    fresh: bool,
    threads: int,
    show: int,
    fail_fast: bool,
    console: rich.console.Console,
):
    # If print_to_cli, it means we only want to print out the view definitions, nothing else
    silent = print_views or silent
    console_log = _do_nothing if silent else console.log

    # List the relevant views
    console_log(f"{len(views):,d} view(s) in total")

    # Organize the views into a directed acyclic graph
    dag = client.make_dag(views)

    # Let's determine which views need to be run.
    if select:
        whitelist = dag.query(select)
        frozen = set()
    else:
        whitelist = set(dag.keys())
        frozen = dag.roots if freeze_roots else set()
    console_log(f"{len(whitelist):,d} view(s) selected")

    # Remove orphan views
    for table_reference in client.list_tables()["table_reference"]:
        view_key = client._reference_to_key(table_reference)
        if view_key in dag:
            continue
        if not dry:
            client.delete_table_reference(table_reference)
        console_log(f"Removed {table_reference}")

    def display_progress() -> rich.table.Table:
        if silent:
            return None
        table = rich.table.Table(box=None)
        table.add_column("#", header_style="italic")
        table.add_column("view", header_style="italic")
        table.add_column("status", header_style="italic")
        table.add_column("duration", header_style="italic")

        not_done = [view_key for view_key in execution_order if view_key not in cache]
        for i, view_key in list(enumerate(not_done, start=1))[-show:]:
            if view_key in jobs_ended_at:
                status = SUCCESS
            elif view_key in exceptions:
                status = ERRORED
            elif view_key in skipped:
                status = SKIPPED
            elif view_key in frozen:
                status = FROZEN
            else:
                status = RUNNING
            duration = (
                (jobs_ended_at.get(view_key, dt.datetime.now()) - jobs_started_at[view_key])
                if view_key in jobs_started_at
                else dt.timedelta(seconds=0)
            )
            rounded_seconds = int(duration.total_seconds())
            table.add_row(str(i), str(dag[view_key]), status, f"{rounded_seconds}s")

        return table

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    jobs = {}
    execution_order = []
    jobs_started_at = {}
    jobs_ended_at = {}
    exceptions = {}
    skipped = set()
    cache_path = pathlib.Path(".cache.pkl")
    cache = set() if fresh or not cache_path.exists() else pickle.loads(cache_path.read_bytes())
    tic = time.time()

    console_log(f"{len(cache):,d} view(s) already done")

    with rich.live.Live(display_progress(), vertical_overflow="visible") as live:
        dag.prepare()
        while dag.is_active():
            for view_key in dag.get_ready():
                # Check if the view_key can be skipped or not
                if view_key not in whitelist:
                    dag.done(view_key)
                    continue
                execution_order.append(view_key)

                # A view can only be computed if all its dependencies have been computed
                # succesfully
                if any(dep in skipped or dep in exceptions for dep in dag[view_key].dependencies):
                    skipped.add(view_key)
                    dag.done(view_key)
                    continue

                # Views that are frozen can be skipped. The views that depend on them will use
                # the frozen version, instead of relying on the output made during this run
                if view_key in frozen:
                    dag.done(view_key)
                    continue

                # Submit a job to create the view
                jobs[view_key] = executor.submit(
                    _do_nothing
                    if dry or view_key in cache
                    else functools.partial(pretty_print_view, view=dag[view_key], console=console)
                    if print_views
                    else functools.partial(client.create, view=dag[view_key])
                )
                jobs_started_at[view_key] = dt.datetime.now()

            # Check if any jobs are done. We notify the DAG by calling done when a job is done,
            # which will unlock the next views.
            for view_key in jobs_started_at:
                if view_key not in jobs_ended_at and jobs[view_key].done():
                    dag.done(view_key)
                    jobs_ended_at[view_key] = dt.datetime.now()
                    # Determine whether the job succeeded or not
                    if exception := jobs[view_key].exception():
                        exceptions[view_key] = exception

            live.update(display_progress())

    # Save the cache
    all_done = not exceptions and not skipped
    cache = (
        set()
        if all_done
        else cache
        | {
            view_key
            for view_key in execution_order
            if view_key not in exceptions and view_key not in skipped
        }
    )
    if cache:
        cache_path.write_bytes(pickle.dumps(cache))
    else:
        cache_path.unlink(missing_ok=True)

    # Summary statistics
    if silent:
        return
    console.log(f"Took {round(time.time() - tic)}s")
    summary = rich.table.Table()
    summary.add_column("status")
    summary.add_column("count")
    if n := len(jobs_ended_at) - len(exceptions):
        summary.add_row(SUCCESS, f"{n:,d}")
    if n := len(exceptions):
        summary.add_row(ERRORED, f"{n:,d}")
    if n := len(skipped):
        summary.add_row(SKIPPED, f"{n:,d}")
    if n := len(frozen):
        summary.add_row(FROZEN, f"{n:,d}")
    console.print(summary)

    # Summary of errors
    if exceptions:
        for view_key, exception in exceptions.items():
            console.print(str(dag[view_key]), style="bold red")
            console.print(exception)

        if fail_fast:
            raise Exception("Some views failed to build")
