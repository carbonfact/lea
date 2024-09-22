from __future__ import annotations

import concurrent.futures
import datetime as dt
import functools
import io
import pathlib
import pickle
import re
import time
import warnings
from typing import Any

import git
import pandas as pd
import rich.console
import rich.live
import rich.table
import sqlglot

import lea
from lea.views.sql import InMemorySQLView, SQLView

console = rich.console.Console(force_interactive=True)

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
        self.views_dir = pathlib.Path(views_dir) if isinstance(views_dir, str) else views_dir
        self.client = client
        self.verbose = verbose

        self.views = {
            view.key: view
            for view in lea.views.open_views(views_dir=self.views_dir, client=self.client)
        }

        self.dag = lea.DAGOfViews(
            graph={view.key: view.dependent_view_keys for view in self.regular_views.values()}
        )
        self.dag.prepare()

    @property
    def regular_views(self):
        """

        What we call regular views are views that are not tests or functions.

        Regular views are the views that are materialized in the database.

        """
        return {
            view_key: view
            for view_key, view in self.views.items()
            if view.schema not in {"tests", "test", "func", "funcs"}
        }

    def log(self, message, **kwargs):
        if self.verbose:
            console.log(message, **kwargs)

    def print(self, message, **kwargs):
        if self.verbose:
            console.print(message, **kwargs)

    def _display_progress(
        self, jobs, jobs_started_at, jobs_ended_at, exceptions, skipped, execution_order, show
    ):
        table = rich.table.Table(box=None)
        table.add_column("#")
        table.add_column("view")
        table.add_column("status")
        table.add_column("duration", justify="right")

        def get_status(view_key):
            if view_key in exceptions:
                return ERRORED
            elif view_key in skipped:
                return SKIPPED
            elif view_key in jobs_ended_at:
                return SUCCESS
            return RUNNING

        not_done = [view_key for view_key in execution_order if view_key not in jobs_ended_at]
        statuses = {view_key: get_status(view_key) for view_key in not_done}
        not_done = [view_key for view_key in not_done if statuses[view_key] != RUNNING] + [
            view_key for view_key in not_done if statuses[view_key] == RUNNING
        ]
        for i, view_key in list(enumerate(not_done, start=1))[-show:]:
            status = statuses[view_key]
            duration = (
                (jobs_ended_at.get(view_key, dt.datetime.now()) - jobs_started_at[view_key])
                if view_key in jobs_started_at
                else None
            )
            duration_str = f"{int(duration.total_seconds())}s" if duration else ""
            table.add_row(
                str(i) if status != RUNNING else "",
                str(self.views[view_key]),
                status,
                duration_str,
            )

        return table

    def select_view_keys(self, *queries: str) -> set:
        """Selects view keys from one or more graph operators.

        The special case where no queries are specified is equivalent to selecting all the views.

        git operators are expanded to select all the views that have been modified compared to the
        main branch.

        """

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
                    if (
                        diff_path.is_relative_to(self.views_dir)
                        and diff_path.name.split(".", 1)[1] in lea.views.PATH_SUFFIXES
                    ):
                        view = lea.views.open_view_from_path(
                            diff_path, self.views_dir, self.client.sqlglot_dialect
                        )
                        yield ("+" if ancestors else "") + str(view) + ("+" if descendants else "")
            else:
                yield query

        if not queries:
            return set(self.regular_views.keys())

        return {
            selected
            for query in queries
            for q in _expand_query(query)
            for selected in self.dag.select(q)
        }

    def _analyze_cte_dependencies(
        self, ctes: dict[str, str], main_query: str
    ) -> dict[str, set[str]]:
        """
        Analyze dependencies between CTEs and the main query.

        Args:
            ctes (Dict[str, str]): A dictionary of CTE names and their corresponding queries.
            main_query (str): The main query string.

        Returns:
            Dict[str, Set[str]]: A dictionary where keys are CTE names (plus 'main' for the main query)
                                and values are sets of CTE names they depend on.
        """
        dependencies: dict[str, set[str]] = {cte_name: set() for cte_name in ctes}
        dependencies["main"] = set()

        for cte_name, cte_query in ctes.items():
            for other_cte in ctes:
                if other_cte in cte_query:
                    dependencies[cte_name].add(other_cte)

        for cte_name in ctes:
            if cte_name in main_query:
                dependencies["main"].add(cte_name)

        return dependencies

    def _create_cte_views(
        self,
        view: SQLView,
        ctes: dict[str, str],
        main_query: str,
        dependencies: dict[str, set[str]],
    ) -> list[InMemorySQLView]:
        """
        Create InMemorySQLView objects for CTEs and the main query.

        This method generates separate views for each CTE (Common Table Expression)
        and the main query. It also sets up the dependencies between these views.

        Args:
            view (SQLView): The original SQLView object.
            ctes (dict[str, str]): A dictionary of CTE names and their corresponding queries.
            main_query (str): The main SQL query string.
            dependencies (dict[str, set[str]]): A dictionary of dependencies between CTEs and the main query.

        Returns:
            list[InMemorySQLView]: A list of InMemorySQLView objects, including all CTE views
            and the main view.

        Note:
            - CTE views are named as "{original_view_name}__{cte_name}".
            - The main view retains the original view's key.
            - Dependencies are set as attributes on each view object.
        """
        cte_views: list[InMemorySQLView] = []
        for cte_name, cte_query in ctes.items():
            cte_view = InMemorySQLView(
                key=(view.key[0], f"{view.key[1]}__{cte_name}"), query=cte_query, client=self.client
            )
            cte_view.dependent_view_keys = dependencies[cte_name]
            cte_views.append(cte_view)

        main_view = InMemorySQLView(key=view.key, query=main_query, client=self.client)
        main_view.dependent_view_keys = dependencies["main"]

        return cte_views + [main_view]

    def _split_query(self, query: str) -> tuple[dict[str, str], str]:
        """
        Split a SQL query into its CTEs and main query.

        Args:
            query (str): The full SQL query string.

        Returns:
            Tuple[Dict[str, str], str]: A tuple containing:
                - A dictionary of CTE names and their corresponding queries.
                - The main query string.
        """
        ast = sqlglot.parse_one(query, dialect=self.client.sqlglot_dialect)
        ctes: dict[str, str] = {}
        main_query = query

        if isinstance(ast, sqlglot.exp.Select):
            with_clause = ast.args.get("with")
            if with_clause and isinstance(with_clause, sqlglot.exp.With):
                for cte in with_clause.expressions:
                    if isinstance(cte, sqlglot.exp.CTE):
                        cte_name = cte.alias
                        cte_query = cte.this.sql(dialect=self.client.sqlglot_dialect)
                        ctes[cte_name] = cte_query

                if "expression" in ast.args:
                    main_query = ast.args["expression"].sql(dialect=self.client.sqlglot_dialect)
                else:
                    main_query = ast.sql(dialect=self.client.sqlglot_dialect)

        return ctes, main_query

    def _make_table_reference_mapping(
        self, selected_view_keys: set[tuple[str]], freeze_unselected: bool
    ) -> dict[str, str]:
        """

        There are two types of table_references: those that refer to a table in the current
        database, and those that refer to a table in another database. This function determine how
        to rename the table references in each view.

        This is important in several cases.

        On the one hand, if you're refreshing views in a developper schema, the table references
        found in each query should be renamed to target the developer schema. For instance, if a
        query contains `FROM dwh.core__users`, we'll want to replace that with
        `FROM dwh_max.core__users`.

        On the other hand, if you're refreshing views in a production, the table references found
        each query should be renamed to target the production schema. In general, this leaves the
        table references untouched.

        A special case is when you're refreshing views in a GitHub Actions workflow. In that case,
        you may only want to refresh views that have been modified compared to the main branch.
        What you'll want to do is rename the table references of modified views, while leaving the
        others untouched. This is the so-called "Slim CI" pattern.

        Note that this method only returns a mapping. It doesn't actually rename the table
        references in a given view. That's a separate responsibility.

        Examples
        --------

        >>> import lea

        >>> client = lea.clients.DuckDB('examples/jaffle_shop/jaffle_shop.db', username='max')
        >>> runner = lea.Runner('examples/jaffle_shop/views', client=client)

        The client has the ability to generate table references from view keys:

        >>> client._view_key_to_table_reference(('core', 'orders'), with_context=False)
        'core.orders'

        >>> client._view_key_to_table_reference(('core', 'orders'), with_context=True)
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

        # Note the case where the select list is empty. That means all the views should be refreshed.
        # If freeze_unselected is specified, then it means all the views will target the production
        # database, which is basically equivalent to copying over the data.
        if freeze_unselected and not selected_view_keys:
            warnings.warn("Setting freeze_unselected without selecting views is not encouraged")

        return {
            self.client._view_key_to_table_reference(
                view_key, with_context=False
            ): self.client._view_key_to_table_reference(view_key, with_context=True)
            for view_key in
            (
                # By default, we replace all
                # table_references to the current database, but we leave the others untouched.
                self.regular_views
                if not freeze_unselected
                # When freeze_unselected is specified, it means we want our views to target the production
                # database. Therefore, we only have to rename the table references for the views that were
                # selected.
                else selected_view_keys
            )
        }

    def prepare(self):
        self.client.prepare(self.regular_views.values())

    def run(
        self,
        select: list[str],
        freeze_unselected: bool,
        print_views: bool,
        dry: bool,
        fresh: bool,
        threads: int,
        show: int,
        fail_fast: bool,
        incremental: bool,
        materialize_ctes: bool,
    ) -> None:
        """
        Run the materialization process for selected views.

        Args:
            select (List[str]): List of view selectors.
            freeze_unselected (bool): Whether to freeze unselected views.
            print_views (bool): Whether to print views instead of materializing them.
            dry (bool): Whether to perform a dry run.
            fresh (bool): Whether to ignore the cache and run all views.
            threads (int): Number of threads to use for parallel execution.
            show (int): Number of views to show in the progress display.
            fail_fast (bool): Whether to stop on the first error.
            incremental (bool): Whether to perform incremental updates.
            materialize_ctes (bool): Whether to materialize CTEs separately.

        Returns:
            None
        """

        tic = time.time()

        # Let's determine which views need to be run
        selected_view_keys: set[tuple] = self.select_view_keys(*(select or []))

        # Let the user know the views we've decided which views will run
        self.log(f"{len(selected_view_keys):,d} out of {len(self.regular_views):,d} views selected")

        # Now we determine the table reference mapping
        table_reference_mapping: dict[str, str] = self._make_table_reference_mapping(
            selected_view_keys=selected_view_keys, freeze_unselected=freeze_unselected
        )

        # Remove orphan views
        existing_view_keys: dict[tuple, str] = self.client.list_existing_view_keys()
        for view_key in set(existing_view_keys.keys()) - selected_view_keys:
            if view_key in self.regular_views:
                continue
            if not dry:
                self.client.delete_table_reference(existing_view_keys[view_key])
            self.log(f"Removed {'.'.join(view_key)}")

        # Handle incremental updates
        if incremental:
            for view_key in selected_view_keys - set(existing_view_keys):
                view = self.regular_views[view_key]
                for i, field in enumerate(view.fields):
                    view._fields[i].tags.discard("#INCREMENTAL")  # HACK

        executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(
            max_workers=threads
        )
        jobs: dict[tuple, Any] = {}
        execution_order: list[tuple] = []
        jobs_started_at: dict[tuple, dt.datetime] = {}
        jobs_ended_at: dict[tuple, dt.datetime] = {}
        exceptions: dict[tuple, Exception] = {}
        skipped: set[tuple] = set()
        cache_path = pathlib.Path(".cache.pkl")
        cache: set[tuple] = (
            set() if fresh or not cache_path.exists() else pickle.loads(cache_path.read_bytes())
        )

        if cache:
            self.log(f"{len(cache):,d} views already done")

        with rich.live.Live(
            self._display_progress(
                jobs, jobs_started_at, jobs_ended_at, exceptions, skipped, execution_order, show
            ),
            vertical_overflow="ellipsis",
        ) as live:
            views_to_materialize: list[SQLView] = []
            for view_key in selected_view_keys:
                view = self.views[view_key].with_context(
                    table_reference_mapping=table_reference_mapping
                )
                if materialize_ctes and isinstance(view, SQLView):
                    # Split the query into CTEs and main query
                    ctes, main_query = self._split_query(view.query)
                    # Analyze dependencies between CTEs and main query
                    dependencies = self._analyze_cte_dependencies(ctes, main_query)
                    # Create separate views for CTEs and main query
                    views_to_materialize.extend(
                        self._create_cte_views(view, ctes, main_query, dependencies)
                    )
                else:
                    views_to_materialize.append(view)

            # Update the DAG with the new views
            for view in views_to_materialize:
                self.dag.add_node(view.key, view.dependent_view_keys)

            while self.dag.is_active():
                for view_key in self.dag.get_ready():
                    # Check if the view_key can be skipped or not
                    if view_key not in selected_view_keys:
                        self.dag.done(view_key)
                        continue
                    execution_order.append(view_key)

                    # A view can only be computed if all its dependencies have been computed successfully
                    if any(
                        dep_key in skipped or dep_key in exceptions
                        for dep_key in self.views[view_key].dependent_view_keys
                    ):
                        skipped.add(view_key)
                        self.dag.done(view_key)
                        continue

                    # Submit a job, or print, or do nothing
                    if dry or view_key in cache:
                        job = self._do_nothing
                    elif print_views:
                        job = functools.partial(
                            console.print,
                            self.views[view_key].with_context(
                                table_reference_mapping=table_reference_mapping
                            ),
                        )
                    else:
                        view = next(v for v in views_to_materialize if v.key == view_key)
                        job = functools.partial(
                            self.client.materialize_view,
                            view=view,
                        )
                    jobs[view_key] = executor.submit(job)
                    jobs_started_at[view_key] = dt.datetime.now()

                # Check if any jobs are done
                for view_key in jobs_started_at:
                    if view_key not in jobs_ended_at and jobs[view_key].done():
                        self.dag.done(view_key)
                        jobs_ended_at[view_key] = dt.datetime.now()
                        # Determine whether the job succeeded or not
                        if exception := jobs[view_key].exception():
                            exceptions[view_key] = exception
                            if fail_fast:
                                raise RuntimeError(
                                    f"Error in {self.views[view_key]}"
                                ) from exception

                live.update(
                    self._display_progress(
                        jobs,
                        jobs_started_at,
                        jobs_ended_at,
                        exceptions,
                        skipped,
                        execution_order,
                        show,
                    )
                )

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

            # In WAP mode, the tables gets created with a suffix to mimic a staging environment. We
            # need to switch the tables to the production environment.
            if self.client.wap_mode and not exceptions and not dry:
                # In WAP mode, we want to guarantee the new tables are correct. Therefore, we run tests
                # on them before switching.
                self.test(
                    select_views=select,
                    freeze_unselected=freeze_unselected,
                    threads=threads,
                    fail_fast=True,
                )
                self.client.switch_for_wap_mode(selected_view_keys)

    def test(self, select_views: list[str], freeze_unselected: bool, threads: int, fail_fast: bool):
        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*(select_views or []))

        # Now we determine the table reference mapping
        table_reference_mapping = self._make_table_reference_mapping(
            selected_view_keys=selected_view_keys,
            freeze_unselected=freeze_unselected,
        )

        # List singular tests
        singular_tests = [
            view.with_context(table_reference_mapping=table_reference_mapping)
            for view in self.views.values()
            if view.schema == "tests"
        ]
        self.log(f"Found {len(singular_tests):,d} singular tests")

        # List assertion tests
        assertion_tests = [
            test for view in self.regular_views.values() for test in view.yield_assertion_tests()
        ]
        self.log(f"Found {len(assertion_tests):,d} assertion tests")

        # Determine which tests need to be run
        tests = [
            test
            for test in singular_tests + assertion_tests
            if
            (
                # Run tests without any dependency whatsoever
                not (test_dependencies := test.dependent_view_keys)
                # Run tests which don't depend on any table in the views directory
                or all(test_dep[0] not in self.dag.schemas for test_dep in test_dependencies)
                # Run tests which have at least one dependency with the selected views
                or any(test_dep in selected_view_keys for test_dep in test_dependencies)
            )
        ]
        self.log(
            f"{len(tests):,d} out of {len(singular_tests) + len(assertion_tests):,d} tests selected"
        )

        # Run tests concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            jobs = {executor.submit(self.client.read_sql, test.query): test for test in tests}
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
                # HACK: skip json views for now
                if str(view.path).endswith("json"):
                    continue

                if view.schema != schema:
                    continue
                content.write(f"### {view}\n\n")
                if view.description:
                    content.write(f"{view.description}\n\n")

                # Write down the query
                content.write(
                    "```sql\n" "SELECT *\n" f"FROM {view.table_reference_in_production}\n" "```\n\n"
                )
                # Write down the columns
                view_columns = pd.DataFrame(
                    {
                        "Column": [field.name for field in view.fields],
                        "Description": [field.description for field in view.fields],
                        "Unique": ["✅" if field.is_unique else "" for field in view.fields],
                    }
                )
                content.write(view_columns.fillna("").to_markdown(index=False) + "\n\n")

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

    def calculate_diff(self, select: list[str], target_client: lea.clients.Client) -> str:
        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*(select or []))

        # HACK
        if not isinstance(self.client, lea.clients.DuckDB):
            selected_table_references = {
                self.client._view_key_to_table_reference(view_key, with_context=False).split(
                    ".", 1
                )[1]
                for view_key in selected_view_keys
            }
        else:
            selected_table_references = None

        schema_diff = lea.diff.get_schema_diff(
            origin_client=self.client, target_client=target_client
        )
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

        table_references = (
            removed_table_references | added_table_references | modified_table_references
        )
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
