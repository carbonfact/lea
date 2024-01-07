from __future__ import annotations

import concurrent.futures
import datetime as dt
import functools
import io
import itertools
import pathlib
import pickle
import re
import time
import warnings

import git
import rich.console
import rich.live
import rich.table

import lea


console = rich.console.Console()

RUNNING = "[cyan]RUNNING"
SUCCESS = "[green]SUCCESS"
ERRORED = "[red]ERRORED"
SKIPPED = "[yellow]SKIPPED"


def _do_nothing(*args, **kwargs):
    """This is a dummy function for dry runs"""


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1000:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1000
    return f"{num:.1f}Yi{suffix}"



class Runner:

    def __init__(self, views_dir: pathlib.Path | str, client: lea.clients.Client, verbose=False):

        if isinstance(views_dir, str):
            views_dir = pathlib.Path(views_dir)
        self.views_dir = views_dir
        self.client = client
        self.verbose = verbose

        self.views = {
            view.key: view
            for view in lea.views.open_views(views_dir=views_dir, sqlglot_dialect=self.client.sqlglot_dialect)
        }
        self.dag = lea.DAGOfViews(
            graph={
                view.key: [
                    self.client._table_reference_to_view_key(table_reference)
                    for table_reference in view.dependencies
                ]
                for view_key, view in self.regular_views.items()
            }
        )

    @property
    def regular_views(self):
        return {
            view_key: view
            for view_key, view in self.views.items()
            if view.schema not in {"tests", "test", "func", "funcs"}
        }

    def log(self, message):
        if self.verbose:
            console.log(message)

    def print(self, message):
        if self.verbose:
            console.print(message)

    def select_view_keys(self, *queries: str) -> set:

        def _expand_query(query):
            # It's possible to query views via git. For example:
            # * `git` will select all the views that have been modified compared to the main branch.
            # * `git+` will select all the modified views, and their descendants.
            # * `+git` will select all the modified views, and their ancestors.
            # * `+git+` will select all the modified views, with their ancestors and descendants.
            if m := re.match(r"(?P<ancestors>\+?)git(?P<descendants>\+?)", query):
                ancestors = m.group("ancestors") == "+"
                descendants = m.group("descendants") == "+"

                repo = git.Repo(".")  # TODO: is using "." always correct? Probably not.
                # Changes that have been committed
                staged_diffs = repo.index.diff(
                    repo.refs.main.commit
                    # repo.remotes.origin.refs.main.commit
                )
                # Changes that have not been committed
                unstage_diffs = repo.head.commit.diff(None)

                for diff in staged_diffs + unstage_diffs:
                    # One thing to note is that we don't filter out deleted views. This is because
                    # these views will get filtered out by dag.select anyway.
                    diff_path = pathlib.Path(diff.a_path)
                    if diff_path.is_relative_to(self.views_dir) and diff_path.name.split(".", 1)[1] in lea.views.PATH_SUFFIXES:
                        view = lea.views.open_view_from_path(
                            diff_path, self.views_dir, self.client.sqlglot_dialect
                        )
                        yield ("+" if ancestors else "") + str(view) + ("+" if descendants else "")
            else:
                yield query

        if not queries:
            return set(self.views.keys())

        return {
            selected
            for query in queries
            for q in _expand_query(query)
            for selected in self.dag.select(q)
        }

    def _make_table_reference_mapping(
        self,
        selected_view_keys: set[tuple[str]],
        freeze_unselected: bool
    ) -> dict[str, str]:
        """

        There are two types of table_references: those that refer to a table in the current database,
        and those that refer to a table in another database. This function determine how to rename the
        table references in each view.

        Examples
        --------

        >>> import lea

        >>> client = lea.clients.DuckDB('examples/jaffle_shop/jaffle_shop.db', username='max')
        >>> runner = lea.Runner('examples/jaffle_shop/views', client=client)

        The client has the ability to generate table references from view keys:

        >>> client._view_key_to_table_reference(('core', 'orders'))
        'core.orders'

        >>> client._view_key_to_table_reference(('core', 'orders'), with_username=True)
        'jaffle_shop_max.core.orders'

        We can use this to generate a mapping that will rename all the table references in the views
        that were selected:

        >>> selected_view_keys = runner.dag.select('core.orders+')
        >>> table_reference_mapping = runner._make_table_reference_mapping(
        ...     selected_view_keys,
        ...     freeze_unselected=True
        ... )

        >>> for name, renamed in sorted(table_reference_mapping.items()):
        ...     print(f'{name} -> {renamed}')
        analytics.finance__kpis -> jaffle_shop_max.analytics.finance__kpis
        analytics.kpis -> jaffle_shop_max.analytics.kpis
        core.orders -> jaffle_shop_max.core.orders

        If `freeze_unselected` is `False`, then all the table references have to be renamed:

        >>> table_reference_mapping = runner._make_table_reference_mapping(
        ...     selected_view_keys,
        ...     freeze_unselected=False
        ... )

        >>> for name, renamed in sorted(table_reference_mapping.items()):
        ...     print(f'{name} -> {renamed}')
        analytics.finance__kpis -> jaffle_shop_max.analytics.finance__kpis
        analytics.kpis -> jaffle_shop_max.analytics.kpis
        core.customers -> jaffle_shop_max.core.customers
        core.orders -> jaffle_shop_max.core.orders
        staging.customers -> jaffle_shop_max.staging.customers
        staging.orders -> jaffle_shop_max.staging.orders
        staging.payments -> jaffle_shop_max.staging.payments

        """

        # By default, we replace all
        # table_references to the current database, but we leave the others untouched.
        if not freeze_unselected:
            return {
                self.client._view_key_to_table_reference(view_key): self.client._view_key_to_table_reference(
                    view_key, with_username=True
                )
                for view_key in self.regular_views
            }

        # When freeze_unselected is specified, it means we want our views to target the production
        # database. Therefore, we only have to rename the table references for the views that were
        # selected.

        # Note the case where the select list is empty. That means all the views should be refreshed.
        # If freeze_unselected is specified, then it means all the views will target the production
        # database, which is basically equivalent to copying over the data.
        if not selected_view_keys:
            warnings.warn("Setting freeze_unselected without selecting views is not encouraged")
        return {
            self.client._view_key_to_table_reference(view_key): self.client._view_key_to_table_reference(
                view_key, with_username=True
            )
            for view_key in selected_view_keys
        }

    def run(
        self,
        select: list[str],
        freeze_unselected: bool,
        print_views: bool,
        dry: bool,
        fresh: bool,
        threads: int,
        show: int,
        fail_fast: bool
    ):

        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*select)

        # Let the user know the views we've decided which views will run
        self.log(f"{len(selected_view_keys):,d} out of {len(self.regular_views):,d} views selected")

        # Now we determine the table reference mapping
        table_reference_mapping = self._make_table_reference_mapping(
            selected_view_keys=selected_view_keys,
            freeze_unselected=freeze_unselected,
        )

        # Remove orphan views
        for table_reference in self.client.list_tables()["table_reference"]:
            view_key = self.client._table_reference_to_view_key(table_reference)
            if view_key in self.regular_views:
                continue
            if not dry:
                client.delete_view_key(view_key)
            self.log(f"Removed {table_reference}")

        def display_progress() -> rich.table.Table:
            if not self.verbose:
                return None
            table = rich.table.Table(box=None)
            table.add_column("#")
            table.add_column("view")
            table.add_column("status")
            table.add_column("duration")

            not_done = [view_key for view_key in execution_order if view_key not in cache]
            for i, view_key in list(enumerate(not_done, start=1))[-show:]:
                if view_key in exceptions:
                    status = ERRORED
                elif view_key in skipped:
                    status = SKIPPED
                elif view_key in jobs_ended_at:
                    status = SUCCESS
                else:
                    status = RUNNING
                duration = (
                    (jobs_ended_at.get(view_key, dt.datetime.now()) - jobs_started_at[view_key])
                    if view_key in jobs_started_at
                    else None
                )
                # Round to the closest second
                duration_str = f"{int(round(duration.total_seconds()))}s" if duration else ""
                table.add_row(str(i), str(self.views[view_key]), status, duration_str)

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

        if cache:
            self.log(f"{len(cache):,d} views already done")

        with rich.live.Live(display_progress(), vertical_overflow="visible") as live:
            self.dag.prepare()
            while self.dag.is_active():
                for view_key in self.dag.get_ready():
                    # Check if the view_key can be skipped or not
                    if view_key not in selected_view_keys:
                        self.dag.done(view_key)
                        continue
                    execution_order.append(view_key)

                    # A view can only be computed if all its dependencies have been computed
                    # succesfully

                    if any(
                        dep_key in skipped or dep_key in exceptions
                        for dep_key in map(
                            self.client._table_reference_to_view_key, self.views[view_key].dependencies
                        )
                    ):
                        skipped.add(view_key)
                        dag.done(view_key)
                        continue

                    # Submit a job, or print, or do nothing
                    if dry or view_key in cache:
                        job = _do_nothing
                    elif print_views:
                        job = functools.partial(
                            console.print,
                            self.views[view_key].rename_table_references(
                                table_reference_mapping=table_reference_mapping
                            )
                        )
                    else:
                        job = functools.partial(
                            self.client.materialize_view,
                            view=self.views[view_key].rename_table_references(
                                table_reference_mapping=table_reference_mapping
                            ),
                        )
                    jobs[view_key] = executor.submit(job)
                    jobs_started_at[view_key] = dt.datetime.now()

                # Check if any jobs are done. We notify the DAG by calling done when a job is done,
                # which will unlock the next views.
                for view_key in jobs_started_at:
                    if view_key not in jobs_ended_at and jobs[view_key].done():
                        self.dag.done(view_key)
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
        if not self.verbose:
            return
        self.log(f"Took {round(time.time() - tic)}s")
        summary = rich.table.Table()
        summary.add_column("status")
        summary.add_column("count")
        if n := len(jobs_ended_at) - len(exceptions):
            summary.add_row(SUCCESS, f"{n:,d}")
        if n := len(exceptions):
            summary.add_row(ERRORED, f"{n:,d}")
        if n := len(skipped):
            summary.add_row(SKIPPED, f"{n:,d}")
        self.print(summary)

        # Summary of errors
        if exceptions:
            for view_key, exception in exceptions.items():
                self.print(str(self.views[view_key]), style="bold red")
                self.print(exception)

            if fail_fast:
                raise Exception("Some views failed to build")

    def test(
        self,
        select_views: list[str],
        freeze_unselected: bool,
        threads: int,
        fail_fast: bool
    ):
        # List all the columns
        columns = self.client.list_columns()

        # List singular tests
        singular_tests = [view for view in self.views.values() if view.schema == "tests"]
        self.log(f"Found {len(singular_tests):,d} singular tests")

        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*select_views)

        # Now we determine the table reference mapping
        table_reference_mapping = self._make_table_reference_mapping(
            selected_view_keys=selected_view_keys,
            freeze_unselected=freeze_unselected,
        )

        # List assertion tests
        assertion_tests = []
        for view in self.regular_views.values():
            # HACK: this is a bit of a hack to get the columns of the view
            view_columns = columns.query(
                f"table_reference == '{self.client._view_key_to_table_reference(view.key, with_username=True)}'"
            )["column"].tolist()
            for test in self.client.discover_assertion_tests(view=view, view_columns=view_columns):
                assertion_tests.append(test)
        self.log(f"Found {len(assertion_tests):,d} assertion tests")

        # Determine which tests need to be run
        tests = [
            test
            for test in singular_tests + assertion_tests
            if
            (
                # Run tests without any dependency whatsoever
                not (
                    test_dependencies := list(
                        map(self.client._table_reference_to_view_key, test.dependencies)
                    )
                )
                # Run tests which don't depend on any table in the views directory
                or all(test_dep[0] not in self.dag.schemas for test_dep in test_dependencies)
                # Run tests which have at least one dependency with the selected views
                or any(test_dep in selected_view_keys for test_dep in test_dependencies)
            )
        ]
        self.log(f"{len(tests):,d} out of {len(singular_tests + assertion_tests):,d} tests selected")

        # Run tests concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            jobs = {
                executor.submit(self.client.load, test): test.rename_table_references(
                    table_reference_mapping=table_reference_mapping
                )
                for test in tests
            }
            for job in concurrent.futures.as_completed(jobs):
                test = jobs[job]
                conflicts = job.result()
                if conflicts.empty:
                    self.log(f"SUCCESS {test}", style="bold green")
                else:
                    self.log(f"FAILURE {test}", style="bold red")
                    self.log(conflicts.head())
                    if fail_fast:
                        # TODO: print out the query to help quick debugging
                        raise RuntimeError(f"Test {test} failed")

    def make_docs(self, output_dir: str):
        output_dir = pathlib.Path(output_dir)

        # List all the columns
        columns = self.client.list_columns()

        # Now we can generate the docs for each schema and view therein
        readme_content = io.StringIO()
        readme_content.write("# Views\n\n")
        readme_content.write("## Schemas\n\n")
        for schema in sorted(self.dag.schemas):
            readme_content.write(f"- [`{schema}`](./{schema})\n")
            content = io.StringIO()

            # Write down the schema description if it exists
            if (existing_readme := self.views_dir / schema / "README.md").exists():
                content.write(existing_readme.read_text() + "\n")
            else:
                content.write(f"# {schema}\n\n")

            # Write down table of contents
            content.write("## Table of contents\n\n")
            for view in sorted(self.regular_views.values(), key=lambda view: view.key):
                if view.schema != schema:
                    continue
                anchor = str(view).replace(".", "")
                content.write(f"- [{view}](#{anchor})\n")
            content.write("\n")

            # Write down the views
            content.write("## Views\n\n")
            for view in sorted(self.regular_views.values(), key=lambda view: view.key):
                if view.schema != schema:
                    continue
                content.write(f"### {view}\n\n")
                if view.description:
                    content.write(f"{view.description}\n\n")

                # Write down the query
                content.write(
                    "```sql\n"
                    "SELECT *\n"
                    f"FROM {self.client._view_key_to_table_reference(view.key)}\n"
                    "```\n\n"
                )
                # Write down the columns
                view_columns = columns.query(
                    f"table_reference == '{self.client._view_key_to_table_reference(view.key, with_username=True)}'"
                )[["column", "type"]]
                view_comments = view.extract_comments(columns=view_columns["column"].tolist())
                view_columns["Description"] = (
                    view_columns["column"]
                    .map(
                        {
                            column: " ".join(
                                comment.text
                                for comment in comment_block
                                if not comment.text.startswith("@")
                            )
                            for column, comment_block in view_comments.items()
                        }
                    )
                    .fillna("")
                )
                view_columns["Unique"] = (
                    view_columns["column"]
                    .map(
                        {
                            column: "✅"
                            if any(comment.text == "@UNIQUE" for comment in comment_block)
                            else ""
                            for column, comment_block in view_comments.items()
                        }
                    )
                    .fillna("")
                )
                view_columns["type"] = view_columns["type"].apply(lambda x: f"`{x}`")
                view_columns = view_columns.rename(columns={"column": "Column", "type": "Type"})
                view_columns = view_columns.sort_values("Column")
                content.write(view_columns.to_markdown(index=False) + "\n\n")

            # Write the schema README
            schema_readme = output_dir / schema / "README.md"
            schema_readme.parent.mkdir(parents=True, exist_ok=True)
            schema_readme.write_text(content.getvalue())
            self.log(f"Wrote {schema_readme}", style="bold green")
        else:
            readme_content.write("\n")

        # Schema flowchart
        mermaid = self.dag.to_mermaid(schemas_only=True)
        mermaid = mermaid.replace("style", "style_")  # HACK
        readme_content.write("## Schema flowchart\n\n")
        readme_content.write(f"```mermaid\n{mermaid}```\n\n")

        # Flowchart
        mermaid = self.dag.to_mermaid()
        mermaid = mermaid.replace("style", "style_")  # HACK
        readme_content.write("## Flowchart\n\n")
        readme_content.write(f"```mermaid\n{mermaid}```\n\n")

        # Write the root README
        readme = output_dir / "README.md"
        readme.parent.mkdir(parents=True, exist_ok=True)
        readme.write_text(readme_content.getvalue())
        self.log(f"Wrote {readme}", style="bold green")

    def calculate_diff(
        self,
        select: set[str],
        target_client: lea.clients.Client
    ) -> str:

        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*select)

        # HACK
        if not isinstance(self.client, lea.clients.DuckDB):
            selected_table_references = {
                self.client._view_key_to_table_reference(view_key).split(".", 1)[1]
                for view_key in selected_view_keys
            }
        else:
            selected_table_references = None

        schema_diff = lea.diff.get_schema_diff(origin_client=self.client, target_client=target_client)
        size_diff = lea.diff.get_size_diff(origin_client=self.client, target_client=target_client)

        removed_table_references = set(
            schema_diff[
                schema_diff.column.isnull() & (schema_diff.diff_kind == "REMOVED")
            ].table_reference
        )
        added_table_references = set(
            schema_diff[
                schema_diff.column.isnull() & (schema_diff.diff_kind == "ADDED")
            ].table_reference
        )
        modified_table_references = set(size_diff.table_reference)

        table_references = removed_table_references | added_table_references | modified_table_references
        if select:
            table_references &= selected_table_references

        if not table_references or (schema_diff.empty and size_diff.empty):
            return "No schema or content change detected."

        buffer = io.StringIO()
        print_ = functools.partial(print, file=buffer)
        for table_reference in sorted(table_references):
            view_schema_diff = schema_diff[
                schema_diff.column.notnull() & schema_diff.table_reference.eq(table_reference)
            ]
            view_size_diff = size_diff[size_diff.table_reference.eq(table_reference)].iloc[0]

            if table_reference in removed_table_references:
                print_(f"- {table_reference}")
            elif table_reference in added_table_references:
                print_(f"+ {table_reference}")
            elif table_reference in modified_table_references:
                print_(f"  {table_reference}")

            if table_reference in modified_table_references:
                # |rows| changed
                if view_size_diff.n_rows_diff:
                    sign = "+" if view_size_diff.n_rows_diff > 0 else "-"
                    print_(f"{sign} {abs(view_size_diff.n_rows_diff):,d} rows")
                # |bytes| changed
                if view_size_diff.n_bytes_diff:
                    sign = "+" if view_size_diff.n_bytes_diff > 0 else "-"
                    print_(f"{sign} {sizeof_fmt(abs(view_size_diff.n_bytes_diff))}")

            for removed in sorted(view_schema_diff[view_schema_diff.diff_kind == "REMOVED"].column):
                print_(f"- {removed}")
            for added in sorted(view_schema_diff[view_schema_diff.diff_kind == "ADDED"].column):
                print_(f"+ {added}")
            print_()

        return buffer.getvalue().rstrip()
