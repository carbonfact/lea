import datetime as dt
import pathlib
import time

import concurrent.futures

import lea
import rich.console

def test(
    client: lea.clients.Client,
    views_dir: str,
    threads: int,
    raise_exceptions: bool,
    console: rich.console.Console,
):

    # The client determines where the views will be written
    # List the test views
    views = lea.views.load_views(views_dir)
    tests = [view for view in views if view.schema == "tests"]
    console.log(f"Found {len(tests):,d} tests")

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        jobs = {executor.submit(client.load, test): test for test in tests}
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
