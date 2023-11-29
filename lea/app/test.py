from __future__ import annotations

import pathlib
import concurrent.futures

import rich.console

import lea

from .run import _determine_selected_view_keys, _make_table_reference_mapping  # HACK


def test(
    client: lea.clients.Client,
    views_dir: pathlib.Path,
    select_views: list[str],
    freeze_unselected: bool,
    threads: int,
    fail_fast: bool,
    console: rich.console.Console,
):
    # List all the columns
    columns = client.list_columns()

    # List singular tests
    views = client.open_views(views_dir)
    singular_tests = [view for view in views if view.schema == "tests"]
    console.log(f"Found {len(singular_tests):,d} singular tests")

    # Let's determine which views need to be run
    regular_views = [view for view in views if view.schema not in {"tests", "func"}]
    dag = client.make_dag(regular_views)
    selected_view_keys = _determine_selected_view_keys(
        dag=dag, select=select_views, client=client, views_dir=views_dir
    )

    # Now we determine the table reference mapping
    table_reference_mapping = _make_table_reference_mapping(
        dag=dag,
        client=client,
        selected_view_keys=selected_view_keys,
        freeze_unselected=freeze_unselected,
    )

    # List assertion tests
    assertion_tests = []
    for view in filter(lambda v: v.schema not in {"funcs", "tests"}, views):
        # HACK: this is a bit of a hack to get the columns of the view
        view_columns = columns.query(
            f"table_reference == '{client._view_key_to_table_reference(view.key, with_username=True)}'"
        )["column"].tolist()
        for test in client.discover_assertion_tests(view=view, view_columns=view_columns):
            assertion_tests.append(test)
    console.log(f"Found {len(assertion_tests):,d} assertion tests")

    # Determine which tests need to be run
    tests = [
        test
        for test in singular_tests + assertion_tests
        if (
            # Run tests without any dependency whatsoever
            not (
                test_dependencies := list(
                    map(client._table_reference_to_view_key, test.dependencies)
                )
            )
            # Run tests which don't depend on any table in the views directory
            or all(test_dep[0] not in dag.schemas for test_dep in test_dependencies)
            # Run tests which have at least one dependency with the selected views
            or any(test_dep in selected_view_keys for test_dep in test_dependencies)
        )
    ]
    tests_sp = "tests" if len(tests) > 1 else "test"
    console.log(f"{len(tests):,d} {tests_sp} selected")

    # Run tests concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        jobs = {
            executor.submit(client.load, test): test.rename_table_references(
                table_reference_mapping=table_reference_mapping
            )
            for test in tests
        }
        for job in concurrent.futures.as_completed(jobs):
            test = jobs[job]
            conflicts = job.result()
            if conflicts.empty:
                console.log(f"SUCCESS {test}", style="bold green")
            else:
                console.log(f"FAILURE {test}", style="bold red")
                console.log(conflicts.head())
                if fail_fast:
                    # TODO: print out the query to help quick debugging
                    raise RuntimeError(f"Test {test} failed")
