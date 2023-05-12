from __future__ import annotations

import abc
import ast
import collections
import concurrent.futures
import dataclasses
import datetime as dt
import functools
import getpass
import glob
import importlib
import io
import itertools
import json
import os
import pathlib
import pickle
import time

import dotenv
import typer
import rich.console
import rich.live
import rich.table

import lea


dotenv.load_dotenv()
app = typer.Typer()
console = rich.console.Console()


@dataclasses.dataclass
class Step:
    n_errors: int = 0
    time_taken: dt.timedelta | None = None

    @property
    def done(self):
        return self.time_taken is not None


@dataclasses.dataclass
class Run:
    started_at: dt.datetime = dataclasses.field(default_factory=dt.datetime.now)
    ended_at: dt.datetime = None
    steps: dict[str, Step] = dataclasses.field(default_factory=dict)

    def __getitem__(self, view_name: str):
        if step := self.steps.get(view_name):
            return step
        self.steps[view_name] = Step()
        return self.steps[view_name]

    def dump(self):
        pathlib.Path(".run.pkl").write_bytes(pickle.dumps(self))

    @classmethod
    def load(cls, fresh):
        if fresh or not (path := pathlib.Path(".run.pkl")).exists():
            return cls()
        return pickle.loads(path.read_bytes())

    def clear(self):
        pathlib.Path(".run.pkl").unlink(missing_ok=True)


def _make_client(username):
    from google.oauth2 import service_account

    return lea.clients.BigQuery(
        credentials=service_account.Credentials.from_service_account_info(
            json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
        ),
        project_id="carbonfact-gsheet",
        dataset_name=os.environ["SCHEMA"],
        username=username,
    )


def _do_nothing():
    """This is a dummy function for dry runs"""


@app.command()
def run(
    views_dir: str,
    only: list[str] = typer.Option(None),
    dry: bool = False,
    rerun: bool = False,
    production: bool = False,
):
    # Massage CLI inputs
    views_dir = pathlib.Path(views_dir)
    if only:
        only = [tuple(v.split(".")) for v in only]

    # Determine the username, who will be the author of this run
    username = None if (production or test) else os.environ.get("USER", getpass.getuser())

    # The client determines where the views will be written
    # TODO: move this to a config file
    client = _make_client(username)

    # List all the relevant views
    views = lea.views.load_views(views_dir)
    console.log(f"Found {len(views):,d} views")

    # Organize the views into a directed acyclic graph
    dag = lea.dag.DAGOfViews(views)

    # Remove orphan views
    for schema, table in client.list_existing():
        if (schema, table) in dag:
            continue
        console.log(f"Removing {schema}.{table}")
        if not dry:
            client.delete(view_name=name)
        console.log(f"Removed {schema}.{table}")

    # Determine which views need to be run
    blacklist = set()
    if only:
        blacklist = set(dag.keys()).difference(only)

    def display_progress() -> rich.table.Table:
        table = rich.table.Table()
        table.add_column("schema")
        table.add_column("view")
        table.add_column("status")
        table.add_column("duration")

        for schema, view_name in jobs:
            status = "[yellow]RUNNING" if (schema, view_name) in jobs_in_progress else "[green]DONE"
            duration = (
                jobs_ended_at.get((schema, view_name), dt.datetime.now())
                - jobs_started_at[(schema, view_name)]
            )
            rounded_seconds = round(duration.total_seconds(), 1)
            table.add_row(schema, view_name, status, f"{rounded_seconds}s")

        return table

    threads = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    dag.prepare()
    jobs = {}
    jobs_in_progress = set()
    jobs_started_at = {}
    jobs_ended_at = {}

    with rich.live.Live(display_progress(), vertical_overflow="visible") as live:
        while dag.is_active():
            # We check if new views have been unlocked
            # If so, we submit a job to create them
            # We record the job in a dict, so that we can check when it's done
            for node in dag.get_ready():
                # Some nodes in the graph are not part of the views, they're external dependencies,
                # so we skip them
                if node not in dag:
                    dag.done(node)
                    continue
                # Some nodes are blacklisted, so we skip them
                if node in blacklist:
                    dag.done(node)
                    continue
                jobs_started_at[node] = dt.datetime.now()
                jobs[node] = threads.submit(
                    functools.partial(client.create, view=dag[node])
                    if not dry
                    else functools.partial(_do_nothing)
                )
                jobs_in_progress.add(node)
            # We check if any jobs are done
            # When a job is done, we notify the DAG, which will unlock the next views
            for node in list(jobs_in_progress):
                if jobs[node].done():
                    dag.done(node)
                    jobs_in_progress.remove(node)
                    jobs_ended_at[node] = dt.datetime.now()
            live.update(display_progress())

    # HACK
    # if production:
    #     account_clients = {
    #         account: BigQuery(
    #             credentials=service_account.Credentials.from_service_account_info(
    #                 json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
    #             ),
    #             project_id="carbonfact-gsheet",
    #             dataset_name=f"export_{account.replace('-', '_')}",
    #             username=None,
    #         )
    #         for account in pathlib.Path(views_dir / "export" / "accounts.txt")
    #         .read_text()
    #         .splitlines()
    #     }

    # # Load/create a run
    # run = Run.load(fresh=rerun)

    # # Determine the execution order
    # order = determine_execution_order(dag, views, start, end, only, inclusive)

    # # Run views
    # for view_key in order:
    #     if not dry:
    #         tic = dt.datetime.now()
    #         try:
    #             if view.schema == "export":
    #                 if production:
    #                     for account in account_clients:
    #                         account_view = GenericSQLView(
    #                             schema="",
    #                             name=view.name,
    #                             query=f"SELECT * FROM (\n{view.query}\n)\nWHERE account = '{account}'",
    #                         )
    #                         console.log(f"Creating {view} for {account}")
    #                         account_clients[account].create(account_view)
    #                 else:
    #                     console.log(f"Skipping {view}")
    #             else:
    #                 console.log(f"Creating {view}")
    #                 client.create(view)
    #             toc = dt.datetime.now()
    #             run[view_key].time_taken = toc - tic
    #         except Exception as e:
    #             console.log(f"Failed creating {view}")
    #             run[view_key].n_errors += 1
    #             run.dump()
    #             raise RuntimeError(view_key) from e
    #     console.log(f"Created {view}")

    # # End the run
    # if dry:
    #     return
    # run.ended_at = dt.datetime.now()
    # # TODO: pretty print summary
    # run.clear()


@app.command()
def test(views_dir: str):
    views_dir = pathlib.Path(views_dir)
    dotenv.load_dotenv()

    # # Test views
    # if test:
    #     tests = [
    #         View.from_path(path)
    #         for path in map(pathlib.Path, glob.glob(f"{views_dir}/tests/**"))
    #         if not path.name.startswith("_") and path.suffix in {".py", ".sql"}
    #     ]
    #     for test in tests:
    #         console.log(test)
    #         if dry:
    #             console.log(str(test))
    #             continue
    #         try:
    #             conflicts = client.load(test)
    #         except Exception as e:
    #             console.log(f"Failed running {test}")
    #             raise e
    #         conflicts = client.load(test)
    #         if not conflicts.empty:
    #             console.log(str(test))
    #             console.log(conflicts)
    #         else:
    #             console.log(str(test))
    #         client.delete(view_name=f"tests__{test.name}")
    #     return


@app.command()
def docs(views_dir: str, output_dir: str = "docs"):
    # Massage CLI inputs
    views_dir = pathlib.Path(views_dir)
    output_dir = pathlib.Path(output_dir)

    # List all the relevant views
    views = lea.views.load_views(views_dir)
    console.log(f"Found {len(views):,d} views")

    # Organize the views into a directed acyclic graph
    dag = lea.dag.DAGOfViews(views)

    # Now we can generate the docs for each schema and view therein
    readme_content = io.StringIO()
    readme_content.write("# Views\n\n")
    readme_content.write("## Schemas\n\n")
    for schema in dag.schemas:
        readme_content.write(f"- [`{schema}`](./{schema})\n")
        content = io.StringIO()

        # Write down the schema description if it exists
        if (existing_readme := views_dir / schema / "README.md").exists():
            content.write(existing_readme.read_text() + "\n")
        else:
            content.write(f"# `{schema}`\n\n")

        # Write down the views
        content.write("## Views\n\n")
        for view in sorted(dag.values()):
            if view.schema != schema:
                continue
            content.write(f"### `{view.name}`\n\n")

        # Write the schema README
        schema_readme = output_dir / schema / "README.md"
        schema_readme.parent.mkdir(parents=True, exist_ok=True)
        schema_readme.write_text(content.getvalue())
    else:
        readme_content.write("\n")

    # Flowchart
    mermaid = dag.to_mermaid()
    mermaid = mermaid.replace("style", "style_")  # HACK
    readme_content.write("## Flowchart\n\n")
    readme_content.write(f"```mermaid\n{mermaid}```\n")

    # Write the root README
    readme = output_dir / "README.md"
    readme.parent.mkdir(parents=True, exist_ok=True)
    readme.write_text(readme_content.getvalue())
