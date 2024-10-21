from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import functools
import io
import pathlib
import pickle
import re
import sys
import time
import warnings

import git
import pandas as pd
import rich.console
import rich.live
import rich.table

import lea

console = rich.console.Console(force_terminal=True)

RUNNING = "[white]RUNNING"
SUCCESS = "[green]SUCCESS"
ERRORED = "[red]ERRORED"
SKIPPED = "[yellow]SKIPPED"
STOPPED = "[cyan]STOPPED"


def _do_nothing(*args, **kwargs):
    """This is a dummy function for dry runs"""


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1000:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1000
    return f"{num:.1f}Yi{suffix}"


@dataclasses.dataclass
class RefreshJob:
    future: concurrent.futures.Future
    view: lea.views.View
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    finished_at: dt.datetime | None = None
    skipped: bool = False
    stopped: bool = False

    @property
    def status(self):
        if self.stopped:
            return STOPPED
        if self.skipped:
            return SKIPPED
        if self.future.done():
            return SUCCESS if self.future.exception() is None else ERRORED
        return RUNNING

    @property
    def error(self) -> BaseException | None:
        if self.future.done() and self.future.exception():
            return self.future.exception()
        return None

    @property
    def cost(self) -> float | None:
        if result := self.future.result():
            return result.cost
        return None


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

        table_reference_mapping = {
            self.client._view_key_to_table_reference(view_key, with_context=False): (
                # When freeze_unselected is specified, it means we want our views to target the production
                # database. Therefore, we only have to rename the table references for the views that were
                # selected.
                (
                    self.client._view_key_to_table_reference(view_key, with_context=True)
                    if view_key in selected_view_keys
                    else self.client._view_key_to_table_reference(
                        view_key, with_context=False, with_project_id=True
                    )
                )
                if freeze_unselected
                else self.client._view_key_to_table_reference(view_key, with_context=True)
            )
            for view_key in self.regular_views
        }

        return {k: v for k, v in table_reference_mapping.items() if v != k}

    def prepare(self):
        self.client.prepare(self.regular_views.values())

    def _make_job(
        self,
        view: lea.views.View,
        executor: concurrent.futures.Executor,
        dry: bool,
        print_views: bool,
    ) -> RefreshJob:
        if dry:
            func = _do_nothing
        elif print_views:
            func = functools.partial(console.print, view)
        else:
            func = functools.partial(self.client.materialize_view, view=view)
        return RefreshJob(future=executor.submit(func), view=view)

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
    ):
        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*(select or []))

        # Let the user know the views we've decided which views will run
        self.log(f"{len(selected_view_keys):,d} out of {len(self.regular_views):,d} views selected")

        # Now we determine the table reference mapping
        table_reference_mapping = self._make_table_reference_mapping(
            selected_view_keys=selected_view_keys, freeze_unselected=freeze_unselected
        )

        # Remove orphan views
        # It's really important to remove views that aren't part of the refresh anymore. There
        # might be consumers that still depend on them, which is error-prone. You don't want
        # consumers to depend on stale data.
        existing_view_keys = self.client.list_existing_view_keys()
        for view_key in set(existing_view_keys.keys()) - selected_view_keys:
            if view_key in self.regular_views:
                continue
            if not dry:
                self.client.delete_table_reference(existing_view_keys[view_key])
            self.log(f"Removed {'.'.join(view_key)}")

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
        jobs: dict[tuple[str, ...], RefreshJob] = {}
        cache_path = pathlib.Path(".cache.pkl")
        cache = set() if fresh or not cache_path.exists() else pickle.loads(cache_path.read_bytes())
        stop = False
        checkpoints = {}
        tic = time.time()

        if cache:
            self.log(f"{len(cache):,d} views already done")

        while self.dag.is_active():
            if stop:
                for job in jobs.values():
                    if job.status == RUNNING:
                        job.future.cancel()
                        job.stopped = True
                        job.finished_at = dt.datetime.now()
                        console.log(f"{STOPPED} {job.view}")
                break

            # Start new jobs
            for view_key in self.dag.get_ready():
                if view_key in jobs and jobs[view_key].finished_at is not None:
                    continue

                # Check if the view_key can be skipped or not
                if view_key not in selected_view_keys:
                    self.dag.done(view_key)
                    continue

                # We can't refresh a view if its dependencies had errors (or were skipped
                # because their dependencies had errors)
                if any(
                    (
                        # Either the dependency had an error
                        (dep_key in jobs and jobs[dep_key].status == ERRORED)
                        # Either the dependency was skipped
                        or (dep_key in selected_view_keys and dep_key not in jobs)
                    )
                    for dep_key in self.views[view_key].dependent_view_keys
                ):
                    self.dag.done(view_key)
                    console.log(f"{SKIPPED} {self.views[view_key]}")
                    continue

                # Submit a job, or print, or do nothing
                job = self._make_job(
                    view=self.views[view_key].with_context(
                        table_reference_mapping=table_reference_mapping
                    ),
                    executor=executor,
                    dry=dry or view_key in cache,
                    print_views=print_views,
                )
                jobs[view_key] = job
                console.log(f"{RUNNING} {job.view}")
                checkpoints[view_key] = job.started_at

            # Check if any jobs are done
            unfinished_jobs = (job for job in jobs.values() if job.finished_at is None)
            for job in unfinished_jobs:
                if job.status == RUNNING:
                    now = dt.datetime.now()
                    if now - checkpoints[job.view.key] > dt.timedelta(seconds=15):
                        duration = now - job.started_at
                        duration_str = f"{int(round(duration.total_seconds()))}s"
                        console.log(f"{RUNNING} {job.view} after {duration_str}")
                        checkpoints[job.view.key] = now
                    continue
                job.finished_at = dt.datetime.now()
                self.dag.done(job.view.key)
                if job.status == ERRORED:
                    console.log(f"{ERRORED} {job.view}", style="red")
                    if fail_fast:
                        stop = True
                        break
                if job.status == SUCCESS:
                    duration = job.finished_at - job.started_at
                    duration_str = f"{int(round(duration.total_seconds()))}s"
                    msg = f"{SUCCESS} {job.view} in {duration_str}"
                    if job.cost:
                        msg += f" for ${job.cost:,.5f}"
                    console.log(msg)

        # Save the cache
        all_jobs_succeeded = all(job.status == SUCCESS for job in jobs.values())
        cache = (
            set()
            if all_jobs_succeeded
            else cache | {view_key for view_key, job in jobs.items() if job.status == SUCCESS}
        )
        if cache:
            cache_path.write_bytes(pickle.dumps(cache))
        else:
            cache_path.unlink(missing_ok=True)

        # Summary statistics
        if not self.verbose:
            return
        self.log(f"Finished in {round(time.time() - tic, 1)}s")

        # Summary of errors
        at_least_one_error = False
        for job in jobs.values():
            if job.error:
                at_least_one_error = True
                self.print(str(job.view), style="bold red")
                self.print(job.error, style="red")
        if at_least_one_error:
            return sys.exit(1)

        # In WAP mode, the tables gets created with a suffix to mimic a staging environment. We
        # need to switch the tables to the production environment.
        if self.client.wap_mode and not all_jobs_succeeded and not dry:
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
            test.with_context(table_reference_mapping=table_reference_mapping)
            for view in self.regular_views.values()
            for test in view.yield_assertion_tests()
        ]
        self.log(f"Found {len(assertion_tests):,d} assertion tests")

        # Determine which tests need to be run
        tests = [
            test
            for test in singular_tests + assertion_tests
            if
            (
                # Run tests without any dependency whatsoever
                not (
                    test_dependencies := {
                        tuple(part for part in view_key if part != "lea_wap")
                        for view_key in test.dependent_view_keys
                    }
                )
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
        at_least_one_error = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            jobs = {executor.submit(self.client.read_sql, test.query): test for test in tests}
            for job in concurrent.futures.as_completed(jobs):
                test = jobs[job]
                try:
                    conflicts = job.result()
                except Exception as e:
                    self.print(f"Error in {test}", style="bold red")
                    self.print(e, style="red")
                    at_least_one_error = True
                if fail_fast and at_least_one_error:
                    for job in jobs:
                        job.cancel()
                    break
                if conflicts.empty:
                    self.log(f"SUCCESS {test}", style="bold green")
                else:
                    self.log(f"FAILURE {test}", style="bold red")
                    self.log(conflicts.head())
                    at_least_one_error = True
                if fail_fast and at_least_one_error:
                    for job in jobs:
                        job.cancel()
                    break

        if at_least_one_error:
            return sys.exit(1)

    def make_docs(self, output_dir: str):
        output_dir_path = pathlib.Path(output_dir)

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
                table_reference = self.client._view_key_to_table_reference(
                    view.key, with_context=False
                )
                content.write("```sql\n" "SELECT *\n" f"FROM {table_reference}\n" "```\n\n")
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
            schema_readme = output_dir_path / schema / "README.md"
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
        readme = output_dir_path / "README.md"
        readme.parent.mkdir(parents=True, exist_ok=True)
        readme.write_text(readme_content.getvalue())
        self.log(f"Wrote {readme}", style="bold green")

    def calculate_diff(self, select: list[str], target_client: lea.clients.Client) -> str:
        # Let's determine which views need to be run
        selected_view_keys = self.select_view_keys(*(select or []))

        if not selected_view_keys:
            return "No schema or content change detected."

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
        if select and selected_table_references:
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
