from __future__ import annotations

import concurrent.futures
import datetime as dt
import functools
import pathlib
import pickle
import time

import rich.console
import rich.live

import lea

SUCCESS = "[green]SUCCESS"
RUNNING = "[yellow]RUNNING"
ERRORED = "[red]ERRORED"
SKIPPED = "[blue]SKIPPED"


def _do_nothing():
    """This is a dummy function for dry runs"""


def run(
    client: lea.clients.Client,
    views_dir: str,
    only: list[str],
    dry: bool,
    fresh: bool,
    threads: int,
    show: int,
    raise_exceptions: bool,
    console: rich.console.Console,
):
    # List the relevant views
    views = lea.views.load_views(views_dir)
    views = [view for view in views if view.schema not in {"tests", "funcs"}]
    console.log(f"{len(views):,d} view(s) in total")

    # Organize the views into a directed acyclic graph
    dag = lea.views.DAGOfViews(views)

    # Determine which views need to be run
    blacklist = set(dag.keys()).difference(only) if only else set()
    console.log(f"{len(views) - len(blacklist):,d} view(s) selected")

    # Remove orphan views
    for schema, table in client.list_existing_view_names():
        if (schema, table) in dag:
            continue
        console.log(f"Removing {schema}.{table}")
        if not dry:
            view_to_delete = lea.views.GenericSQLView(schema=schema, name=table, query="")
            client.delete_view(view=view_to_delete)
        console.log(f"Removed {schema}.{table}")

    def display_progress() -> rich.table.Table:
        table = rich.table.Table(box=None)
        table.add_column("#", header_style="italic")
        table.add_column("schema", header_style="italic")
        table.add_column("view", header_style="italic")
        table.add_column("status", header_style="italic")
        table.add_column("duration", header_style="italic")

        order_not_done = [node for node in order if node not in cache]
        for i, (schema, view_name) in list(enumerate(order_not_done, start=1))[-show:]:
            status = SUCCESS if (schema, view_name) in jobs_ended_at else RUNNING
            status = ERRORED if (schema, view_name) in exceptions else status
            status = SKIPPED if (schema, view_name) in skipped else status
            duration = (
                (
                    jobs_ended_at.get((schema, view_name), dt.datetime.now())
                    - jobs_started_at[(schema, view_name)]
                )
                if (schema, view_name) in jobs_started_at
                else dt.timedelta(seconds=0)
            )
            rounded_seconds = round(duration.total_seconds(), 1)
            table.add_row(str(i), schema, view_name, status, f"{rounded_seconds}s")

        return table

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    jobs = {}
    order = []
    jobs_started_at = {}
    jobs_ended_at = {}
    exceptions = {}
    skipped = set()
    cache_path = pathlib.Path(".cache.pkl")
    cache = (
        set()
        if fresh or not cache_path.exists()
        else pickle.loads(cache_path.read_bytes())
    )
    tic = time.time()

    console.log(f"{len(cache):,d} view(s) already done")

    with rich.live.Live(
        display_progress(), vertical_overflow="visible", refresh_per_second=6
    ) as live:
        dag.prepare()
        while dag.is_active():
            # We check if new views have been unlocked
            # If so, we submit a job to create them
            # We record the job in a dict, so that we can check when it's done
            for node in dag.get_ready():
                if (
                    # Some nodes in the graph are not part of the views,
                    # they're external dependencies which can be ignored
                    node not in dag
                    # Some nodes are blacklisted, so we skip them
                    or node in blacklist
                ):
                    dag.done(node)
                    continue

                order.append(node)

                # A node can only be computed if all its dependencies have been computed
                # If all the dependencies have not been computed succesfully, we skip the node
                if any(
                    dep in skipped or dep in exceptions
                    for dep in dag[node].dependencies
                ):
                    skipped.add(node)
                    dag.done(node)
                    continue

                jobs[node] = executor.submit(
                    _do_nothing
                    if dry or node in cache
                    else functools.partial(client.create, view=dag[node])
                )
                jobs_started_at[node] = dt.datetime.now()
            # We check if any jobs are done
            # When a job is done, we notify the DAG, which will unlock the next views
            for node in jobs_started_at:
                if node not in jobs_ended_at and jobs[node].done():
                    dag.done(node)
                    jobs_ended_at[node] = dt.datetime.now()
                    # Determine whether the job succeeded or not
                    if exception := jobs[node].exception():
                        exceptions[node] = exception
            live.update(display_progress())

    # Save the cache
    all_done = not exceptions and not skipped
    cache = (
        set()
        if all_done
        else cache
        | {node for node in order if node not in exceptions and node not in skipped}
    )
    if cache:
        cache_path.write_bytes(pickle.dumps(cache))
    else:
        cache_path.unlink(missing_ok=True)

    # Summary statistics
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
    console.print(summary)

    # Summary of errors
    if exceptions:
        for (schema, view_name), exception in exceptions.items():
            console.print(f"{schema}.{view_name}", style="bold red")
            console.print(exception)

        if raise_exceptions:
            raise Exception("Some views failed to build")
