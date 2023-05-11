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

import dataclasses_json
import dotenv
import jinja2
import networkx as nx
import sqlglot
import typer
from google.cloud import bigquery
from google.oauth2 import service_account
from rich.console import Console


@dataclasses.dataclass
class View(abc.ABC):
    path: pathlib.Path

    def __post_init__(self):
        if not isinstance(self.path, pathlib.Path):
            self.path = pathlib.Path(self.path)

    @property
    def schema(self):
        return list(self.path.parents)[:-2][-1].name

    @property
    def name(self):
        parents = itertools.takewhile(
            lambda x: x != self.schema, (p.stem for p in self.path.parents)
        )
        name_parts = itertools.chain(parents, [self.path.stem])
        return "__".join(name_parts)

    def __repr__(self):
        return f"{self.schema}.{self.name}"

    @classmethod
    def from_path(cls, path):
        if path.suffix == ".py":
            return PythonView(path)
        if path.suffix == ".sql":
            return SQLView(path)

    @property
    @abc.abstractmethod
    def dependencies(self) -> set[str]:
        ...


class SQLView(View):
    @property
    def query(self):
        text = self.path.read_text().rstrip().rstrip(";")
        if text.startswith("{% extends"):
            views_dir = list(self.path.parents)[-2]
            environment = jinja2.Environment(loader=jinja2.FileSystemLoader(views_dir))
            template = environment.get_template(str(self.path.relative_to(views_dir)))
            return template.render()
        return text

    @classmethod
    def _parse_dependencies(cls, sql):
        parse = sqlglot.parse_one(sql)
        cte_names = {(None, cte.alias) for cte in parse.find_all(sqlglot.exp.CTE)}
        # HACK: can be fixed once we have one dataset per schema
        table_names = {
            (table.sql().split(".")[0], table.name)
            if "__" not in table.name and "." in table.sql()
            else (table.name.split("__")[0], table.name.split("__")[1])
            if "__" in table.name
            else (None, table.name)
            for table in parse.find_all(sqlglot.exp.Table)
        }
        return table_names - cte_names

    @property
    def dependencies(self):
        # HACK: sqlglot can't parse these views
        if self.schema == "core" and self.name == "measured_carbonverses_measurements":
            return {("core", "measured_carbonverses"), ("core", "indicators")}
        if self.schema == "core" and self.name == "carbonverses":
            return {("core", "measured_carbonverses")}
        if self.schema == "core" and self.name == "components":
            return {("core", "measured_carbonverses")}
        if self.schema == "core" and self.name == "materials":
            return {("core", "components")}
        if self.schema == "core" and self.name == "emission_factors":
            return {("niklas", "emission_factor_snapshot_records")}
        if self.schema == "collect" and self.name == "material_funnel":
            return {
                ("core", "measured_carbonverses"),
                ("core", "products"),
                ("core", "footprints"),
                ("core", "materials_measurements"),
            }
        if self.schema == "core" and self.name == "transport_steps":
            return {("core", "measured_carbonverses")}
        if self.schema == "platform" and self.name == "events":
            return {("posthog", "events")}
        if self.schema == "core" and self.name == "modifiers":
            return {("core", "materials")}
        return self._parse_dependencies(self.query)


class GenericSQLView(SQLView):
    def __init__(self, schema, name, query):
        self._schema = schema
        self._name = name
        self._query = query

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self._name

    @property
    def query(self):
        return self._query


class PythonView(View):
    @property
    def dependencies(self):
        def _dependencies():

            code = self.path.read_text()
            for node in ast.walk(ast.parse(code)):
                # pd.read_gbq
                try:
                    if (
                        isinstance(node, ast.Call)
                        and node.func.value.id == "pd"
                        and node.func.attr == "read_gbq"
                    ):
                        yield from SQLView._parse_dependencies(node.args[0].value)
                except AttributeError:
                    pass

                # .query
                try:
                    if isinstance(node, ast.Call) and node.func.attr.startswith(
                        "query"
                    ):
                        yield from SQLView._parse_dependencies(node.args[0].value)
                except AttributeError:
                    pass

        return set(_dependencies())


class DAGOfViews(nx.DiGraph):
    def __init__(self, views: list[View] = None):
        super().__init__(
            (dependency, (view.schema, view.name))
            for view in views or []
            for dependency in view.dependencies
        )
        # Some views have no dependencies but still have to be included
        for view in views or []:
            self.add_node((view.schema, view.name))


class Client(abc.ABC):
    @abc.abstractmethod
    def _create_sql(self, view: SQLView):
        ...

    @abc.abstractmethod
    def _create_python(self, view: PythonView):
        ...

    def create(self, view: View):
        if isinstance(view, SQLView):
            return self._create_sql(view)
        elif isinstance(view, PythonView):
            return self._create_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def _load_sql(self, view: SQLView):
        ...

    def _load_python(self, view: PythonView):
        # HACK
        mod = importlib.import_module("views")
        output = getattr(mod, view.name).main()
        return output

    def load(self, view: View):
        if isinstance(view, SQLView):
            return self._load_sql(view)
        elif isinstance(view, PythonView):
            return self._load_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def list_existing(self, schema: str) -> list[str]:
        ...

    @abc.abstractmethod
    def delete(self, view_name: str):
        ...


class BigQuery(Client):
    def __init__(self, credentials, project_id, dataset_name, username):
        self.project_id = project_id
        self.client = bigquery.Client(credentials=credentials)
        self._dataset_name = dataset_name
        self.username = username

    @property
    def dataset_name(self):
        return (
            f"{self._dataset_name}_{self.username}"
            if self.username
            else self._dataset_name
        )

    def _make_job(self, view: SQLView):

        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")

        return self.client.create_job(
            {
                "query": {
                    "query": query,
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_name,
                        "tableId": f"{view.schema}__{view.name}".lstrip("_"),
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            }
        )

    def _create_sql(self, view: SQLView):
        job = self._make_job(view)
        job.result()

    def _create_python(self, view: PythonView):
        output = self._load_python(view)

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            output,
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}",
            job_config=job_config,
        )
        job.result()

    def _load_sql(self, view: SQLView):
        job = self._make_job(view)
        return job.to_dataframe()

    def list_existing(self):
        return [table.table_id for table in self.client.list_tables(self.dataset_name)]

    def delete(self, view_name: str):
        self.client.delete_table(f"{self.project_id}.{self.dataset_name}.{view_name}")


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Step:
    n_errors: int = 0
    time_taken: dt.timedelta | None = None

    @property
    def done(self):
        return self.time_taken is not None


@dataclasses_json.dataclass_json
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


def main(
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

    # Parse CLI inputs
    if only:
        only = [tuple(v.split(".")) for v in only]
    if start:
        start = tuple(start.split("."))
    if end:
        end = tuple(end.split("."))

    # The client determines where the views will be written
    client = BigQuery(
        credentials=service_account.Credentials.from_service_account_info(
            json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
        ),
        project_id="carbonfact-gsheet",
        dataset_name=os.environ["SCHEMA"],
        username=None
        if (production or test)
        else os.environ.get("USER", getpass.getuser()),
    )
    if production:
        account_clients = {
            account: BigQuery(
                credentials=service_account.Credentials.from_service_account_info(
                    json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
                ),
                project_id="carbonfact-gsheet",
                dataset_name=f"export_{account.replace('-', '_')}",
                username=None,
            )
            for account in pathlib.Path(views_dir / "export" / "accounts.txt")
            .read_text()
            .splitlines()
        }

    # Test views
    if test:
        tests = [
            View.from_path(path)
            for path in map(pathlib.Path, glob.glob(f"{views_dir}/tests/**"))
            if not path.name.startswith("_") and path.suffix in {".py", ".sql"}
        ]
        for test in tests:
            console.log(test)
            if dry:
                console.log(str(test))
                continue
            try:
                conflicts = client.load(test)
            except Exception as e:
                console.log(f"Failed running {test}")
                raise e
            conflicts = client.load(test)
            if not conflicts.empty:
                console.log(str(test))
                console.log(conflicts)
            else:
                console.log(str(test))
            client.delete(view_name=f"tests__{test.name}")
        return

    # Load/create a run
    run = Run.load(fresh=rerun)

    # Enumerate the views
    schema_dirs = [p for p in views_dir.iterdir() if p.is_dir()]
    all_views = [
        View.from_path(path)
        for schema_dir in schema_dirs
        for path in schema_dir.rglob("*")
        if not path.is_dir()
        and not path.name.startswith("_")
        and path.suffix in {".py", ".sql"}
        and path.stat().st_size > 0
    ]
    views = [view for view in all_views if view.schema not in {"tests", "stale", "funcs"}]

    # Organize the views into a directed acyclic graph
    dag = DAGOfViews(views)
    views = {(view.schema, view.name): view for view in views}

    # Determine the execution order
    order = determine_execution_order(dag, views, start, end, only, inclusive)

    # Visualize dependencies
    if viz:
        dot = to_graphviz(dag, views, order)
        dot.render(view=True, cleanup=True)
        return

    # Removing orphan views
    for name in client.list_existing():
        # HACK: can be fixed once we have one dataset per schema
        schema, table = name.split("__", 1)
        if (schema, table) in views:
            continue
        console.log(f"Removing {schema}.{table}")
        if not dry:
            client.delete(view_name=name)
        console.log(f"Removed {schema}.{table}")

    # Run views
    for view_key in order:
        if not (view := views.get(view_key)):
            continue
        if run[view_key].done:
            console.log(f"Skipping {view}")
            continue
        if not dry:
            tic = dt.datetime.now()
            try:
                if view.schema == "export":
                    if production:
                        for account in account_clients:
                            account_view = GenericSQLView(
                                schema="",
                                name=view.name,
                                query=f"SELECT * FROM (\n{view.query}\n)\nWHERE account = '{account}'",
                            )
                            console.log(f"Creating {view} for {account}")
                            account_clients[account].create(account_view)
                    else:
                        console.log(f"Skipping {view}")
                else:
                    console.log(f"Creating {view}")
                    client.create(view)
                toc = dt.datetime.now()
                run[view_key].time_taken = toc - tic
            except Exception as e:
                console.log(f"Failed creating {view}")
                run[view_key].n_errors += 1
                run.dump()
                raise RuntimeError(view_key) from e
        console.log(f"Created {view}")

    # End the run
    if dry:
        return
    run.ended_at = dt.datetime.now()
    # TODO: pretty print summary
    run.clear()


if __name__ == "__main__":
    typer.run(main)
