from __future__ import annotations

import abc
import ast
import dataclasses
import datetime as dt
import getpass
import glob
import importlib
import itertools
import json
import os
import pathlib
import pickle

import dotenv
import jinja2
import networkx as nx
import sqlglot
import typer
from google.cloud import bigquery
from google.oauth2 import service_account
from rich.console import Console

from . import clients


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


def determine_execution_order(dag, views, start, end, only, inclusive):
    if only:
        return only

    if start:
        order = [start] if inclusive else []
        for src, dst in nx.bfs_edges(dag, start):
            if dst not in order:
                order.append(dst)
        return order

    if end:
        subset = {end} if inclusive else []
        for src, dst in nx.bfs_edges(dag, end, reverse=True):
            if dst not in subset and dst in views:
                subset.add(dst)
        order = list(nx.topological_sort(dag.subgraph(subset)))
        return order

    return list(nx.topological_sort(dag))


def to_graphviz(dag, views, order):
    import graphviz

    dot = graphviz.Digraph()

    for node in dag.nodes:
        style = {}
        # Source tables
        if node not in views.keys():
            style["shape"] = "box"
        # Views
        else:
            if isinstance(views[node], PythonView):
                style["color"] = "darkgoldenrod2"
                style["fontcolor"] = "dodgerblue4"
        if node in views.keys() and node in order:
            style["style"] = "filled"
            style["fillcolor"] = "lightgreen"
        dot.node(".".join(node), **style)

    # Dependencies
    for dst in dag.nodes:
        for src in dag.predecessors(dst):
            dot.edge(".".join(src), ".".join(dst))

    return dot



app = typer.Typer()


@app.command()
def run(
    views_dir: str,
    only: list[str] = typer.Option(None),
    start: str = typer.Option(None),
    end: str = typer.Option(None),
    inclusive: bool = True,
    dry: bool = False,
    viz: bool = False,
    test: bool = False,
    rerun: bool = False,
    production: bool = False,
):

    views_dir = pathlib.Path(views_dir)
    console = Console()
    dotenv.load_dotenv()

    # Massage CLI inputs into tuples
    if only:
        only = [tuple(v.split(".")) for v in only]
    if start:
        start = tuple(start.split("."))
    if end:
        end = tuple(end.split("."))

    # The client determines where the views will be written
    # TODO: move this to a config file
    client = clients.BigQuery(
        credentials=service_account.Credentials.from_service_account_info(
            json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
        ),
        project_id="carbonfact-gsheet",
        dataset_name=os.environ["SCHEMA"],
        username=None
        if (production or test)
        else os.environ.get("USER", getpass.getuser()),
    )
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

    # # Load/create a run
    # run = Run.load(fresh=rerun)

    # # Enumerate the views
    # schema_dirs = [p for p in views_dir.iterdir() if p.is_dir()]
    # all_views = [
    #     View.from_path(path)
    #     for schema_dir in schema_dirs
    #     for path in schema_dir.rglob("*")
    #     if not path.is_dir()
    #     and not path.name.startswith("_")
    #     and path.suffix in {".py", ".sql"}
    #     and path.stat().st_size > 0
    # ]
    # views = [view for view in all_views if view.schema not in {"tests", "stale", "funcs"}]

    # # Organize the views into a directed acyclic graph
    # dag = DAGOfViews(views)
    # views = {(view.schema, view.name): view for view in views}

    # # Determine the execution order
    # order = determine_execution_order(dag, views, start, end, only, inclusive)

    # # Visualize dependencies
    # if viz:
    #     dot = to_graphviz(dag, views, order)
    #     dot.render(view=True, cleanup=True)
    #     return

    # # Removing orphan views
    # for name in client.list_existing():
    #     # HACK: can be fixed once we have one dataset per schema
    #     schema, table = name.split("__", 1)
    #     if (schema, table) in views:
    #         continue
    #     console.log(f"Removing {schema}.{table}")
    #     if not dry:
    #         client.delete(view_name=name)
    #     console.log(f"Removed {schema}.{table}")

    # # Run views
    # for view_key in order:
    #     if not (view := views.get(view_key)):
    #         continue
    #     if run[view_key].done:
    #         console.log(f"Skipping {view}")
    #         continue
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

    # The client determines where the views will be written
    ...
