from __future__ import annotations

import concurrent.futures

import rich.console

import lea


def test(
    client: lea.clients.Client,
    views_dir: str,
    threads: int,
    raise_exceptions: bool,
    console: rich.console.Console,
):
    # List all the columns
    columns = client.get_columns()

    # The client determines where the views will be written
    # List the test views
    views = lea.views.load_views(views_dir, sqlglot_dialect=client.sqlglot_dialect)
    singular_tests = [view for view in views if view.schema == "tests"]
    console.log(f"Found {len(singular_tests):,d} singular tests")

    generic_tests = []
    for view in views:
        view_columns = columns.query(f"table == '{view.schema}__{view.name}'")[
            "column"
        ].tolist()
        for generic_test in client.yield_unit_tests(view=view, view_columns=view_columns):
            generic_tests.append(generic_test)
    console.log(f"Found {len(generic_tests):,d} generic tests")

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        jobs = {
            executor.submit(client.load, test): test
            for test in singular_tests + generic_tests
        }
        for job in concurrent.futures.as_completed(jobs):
            test = jobs[job]
            conflicts = job.result()
            if conflicts.empty:
                console.log(f"SUCCESS {test}", style="bold green")
            else:
                console.log(f"FAILURE {test}", style="bold red")
                console.log(conflicts.head())
                if raise_exceptions:
                    raise RuntimeError(f"Test {test} failed")
