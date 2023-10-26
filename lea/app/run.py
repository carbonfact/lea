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

SUCCESS = "[green]SUCCESS"
RUNNING = "[yellow]RUNNING"
ERRORED = "[red]ERRORED"
SKIPPED = "[blue]SKIPPED"


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


def make_whitelist(query: str, dag: lea.views.DAGOfViews) -> set:
    """Make a whitelist of tables given a query.

    These are the different queries to handle:

    schema.table
    schema.table+   (descendants)
    +schema.table   (ancestors)
    +schema.table+  (ancestors and descendants)
    schema/         (all tables in schema)
    schema/+        (all tables in schema with their descendants)
    +schema/        (all tables in schema with their ancestors)
    +schema/+       (all tables in schema with their ancestors and descendants)

    Examples
    --------

    >>> import lea

    >>> views = lea.views.load_views('examples/jaffle_shop/views', sqlglot_dialect='duckdb')
    >>> views = [v for v in views if v.schema != 'tests']
    >>> dag = lea.views.DAGOfViews(views)

    >>> def pprint(whitelist):
    ...     for key in sorted(whitelist):
    ...         print('.'.join(key))

    schema.table

    >>> pprint(make_whitelist('staging.orders', dag))
    staging.orders

    schema.table+ (descendants)

    >>> pprint(make_whitelist('staging.orders+', dag))
    analytics.finance.kpis
    analytics.kpis
    core.customers
    core.orders
    staging.orders

    +schema.table (ancestors)

    >>> pprint(make_whitelist('+core.customers', dag))
    core.customers
    staging.customers
    staging.orders
    staging.payments

    +schema.table+ (ancestors and descendants)

    >>> pprint(make_whitelist('+core.customers+', dag))
    analytics.kpis
    core.customers
    staging.customers
    staging.orders
    staging.payments

    schema/ (all tables in schema)

    >>> pprint(make_whitelist('staging/', dag))
    staging.customers
    staging.orders
    staging.payments

    schema/+ (all tables in schema with their descendants)

    >>> pprint(make_whitelist('staging/+', dag))
    analytics.finance.kpis
    analytics.kpis
    core.customers
    core.orders
    staging.customers
    staging.orders
    staging.payments

    +schema/ (all tables in schema with their ancestors)

    >>> pprint(make_whitelist('+core/', dag))
    core.customers
    core.orders
    staging.customers
    staging.orders
    staging.payments

    +schema/+  (all tables in schema with their ancestors and descendants)

    >>> pprint(make_whitelist('+core/+', dag))
    analytics.finance.kpis
    analytics.kpis
    core.customers
    core.orders
    staging.customers
    staging.orders
    staging.payments

    schema.subschema/

    >>> pprint(make_whitelist('analytics.finance/', dag))
    analytics.finance.kpis

    """

    def _yield_whitelist(query, include_ancestors, include_descendants):
        if query.endswith("+"):
            yield from _yield_whitelist(
                query[:-1], include_ancestors=include_ancestors, include_descendants=True
            )
            return
        if query.startswith("+"):
            yield from _yield_whitelist(
                query[1:], include_ancestors=True, include_descendants=include_descendants
            )
            return
        if query.endswith("/"):
            for key in dag:
                if str(dag[key]).startswith(query[:-1]):
                    yield from _yield_whitelist(
                        ".".join(key),
                        include_ancestors=include_ancestors,
                        include_descendants=include_descendants,
                    )
        else:
            key = tuple(query.split("."))
            yield key
            if include_ancestors:
                yield from dag.list_ancestors(key)
            if include_descendants:
                yield from dag.list_descendants(key)

    return set(_yield_whitelist(query, include_ancestors=False, include_descendants=False))


def run(
    client: lea.clients.Client,
    views: list[lea.views.View],
    only: list[str],
    dry: bool,
    print_to_cli: bool,
    fresh: bool,
    threads: int,
    show: int,
    raise_exceptions: bool,
    console: rich.console.Console,
):
    # If print_to_cli, it means we only want to print out the view definitions, nothing else
    console_log = _do_nothing if print_to_cli else console.log

    # List the relevant views
    console_log(f"{len(views):,d} view(s) in total")

    # Organize the views into a directed acyclic graph
    dag = lea.views.DAGOfViews(views)
    dag.prepare()

    # Determine which views need to be run
    whitelist = (
        set.union(*(make_whitelist(query, dag) for query in only)) if only else set(dag.keys())
    )
    console_log(f"{len(whitelist):,d} view(s) selected")

    # Remove orphan views
    for schema, table in client.list_existing_view_names():
        if (schema, table) in dag:
            continue
        console_log(f"Removing {schema}.{table}")
        if not dry:
            view_to_delete = lea.views.GenericSQLView(
                schema=schema, name=table, query="", sqlglot_dialect=client.sqlglot_dialect
            )
            client.delete_view(view=view_to_delete)
        console_log(f"Removed {schema}.{table}")

    def display_progress() -> rich.table.Table:
        if print_to_cli:
            return None
        table = rich.table.Table(box=None)
        table.add_column("#", header_style="italic")
        table.add_column("view", header_style="italic")
        table.add_column("status", header_style="italic")
        table.add_column("duration", header_style="italic")

        order_not_done = [node for node in order if node not in cache]
        for i, node in list(enumerate(order_not_done, start=1))[-show:]:
            status = SUCCESS if node in jobs_ended_at else RUNNING
            status = ERRORED if node in exceptions else status
            status = SKIPPED if node in skipped else status
            duration = (
                (jobs_ended_at.get(node, dt.datetime.now()) - jobs_started_at[node])
                if node in jobs_started_at
                else dt.timedelta(seconds=0)
            )
            rounded_seconds = round(duration.total_seconds(), 1)
            table.add_row(str(i), str(dag[node]), status, f"{rounded_seconds}s")

        return table

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    jobs = {}
    order = []
    jobs_started_at = {}
    jobs_ended_at = {}
    exceptions = {}
    skipped = set()
    cache_path = pathlib.Path(".cache.pkl")
    cache = set() if fresh or not cache_path.exists() else pickle.loads(cache_path.read_bytes())
    tic = time.time()

    console_log(f"{len(cache):,d} view(s) already done")

    with rich.live.Live(
        display_progress(), vertical_overflow="visible", refresh_per_second=6
    ) as live:
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
                    or node not in whitelist
                ):
                    dag.done(node)
                    continue

                order.append(node)

                # A node can only be computed if all its dependencies have been computed
                # If all the dependencies have not been computed succesfully, we skip the node
                if any(dep in skipped or dep in exceptions for dep in dag[node].dependencies):
                    skipped.add(node)
                    dag.done(node)
                    continue

                jobs[node] = executor.submit(
                    _do_nothing
                    if dry or node in cache
                    else functools.partial(pretty_print_view, view=dag[node], console=console)
                    if print_to_cli
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
        else cache | {node for node in order if node not in exceptions and node not in skipped}
    )
    if cache:
        cache_path.write_bytes(pickle.dumps(cache))
    else:
        cache_path.unlink(missing_ok=True)

    # Summary statistics
    if print_to_cli:
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
    console.print(summary)

    # Summary of errors
    if exceptions:
        for node, exception in exceptions.items():
            console.print(str(dag[node]), style="bold red")
            console.print(exception)

        if raise_exceptions:
            raise Exception("Some views failed to build")
