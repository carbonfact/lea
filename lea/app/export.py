from __future__ import annotations

import concurrent.futures
import functools
import json
import os
import pathlib

import rich.console

import lea


def export(
    views_dir: str,
    threads: int,
    client: lea.clients.Client,
    console: rich.console.Console,
):
    # Massage CLI inputs
    views_dir = pathlib.Path(views_dir)

    # List the export views
    views = lea.views.load_views(views_dir)
    views = [view for view in views if view.schema == "export"]
    console.log(f"{len(views):,d} view(s) in total")

    # List the accounts for which to produce exports
    accounts = (
        pathlib.Path(views_dir / "export" / "accounts.txt").read_text().splitlines()
    )
    console.log(f"{len(accounts):,d} account(s) in total")

    from google.oauth2 import service_account

    from lea.clients.bigquery import BigQuery

    account_clients = {
        account: BigQuery(
            credentials=service_account.Credentials.from_service_account_info(
                json.loads(os.environ["LEA_BQ_SERVICE_ACCOUNT"])
            ),
            project_id="carbonfact-gsheet",
            location="EU",
            dataset_name=f"export_{account.replace('-', '_')}",
            username=None,
        )
        for account in accounts
    }

    # Need to create datasets first
    for account in account_clients:
        account_clients[account].prepare(console)

    def export_view_for_account(view, account):
        account_export = lea.views.GenericSQLView(
            schema="",
            name=view.name,
            query=f"""
            SELECT * EXCEPT (account_slug)
            FROM (
                {view.query}
            )
            WHERE account_slug = '{account}'
            """,
        )
        account_clients[account].create(account_export)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        jobs = {
            executor.submit(
                functools.partial(export_view_for_account, view=view, account=account)
            ): (view, account)
            for view in views
            for account in account_clients
        }
        for job in concurrent.futures.as_completed(jobs):
            view, account = jobs[job]
            if exc := job.exception():
                console.log(f"Failed exporting {view} for {account}", style="bold red")
                console.log(exc, style="bold magenta")
            else:
                console.log(f"Exported {view.name} for {account}", style="bold green")
