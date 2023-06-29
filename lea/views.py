from __future__ import annotations

import abc
import ast
import dataclasses
import itertools
import pathlib
import re

import jinja2
import sqlglot


@dataclasses.dataclass
class View(abc.ABC):
    origin: pathlib.Path
    relative_path: pathlib.Path

    @property
    def path(self):
        return self.origin.joinpath(self.relative_path)

    def __post_init__(self):
        if not isinstance(self.path, pathlib.Path):
            self.path = pathlib.Path(self.path)

    @property
    def schema(self):
        return self.relative_path.parts[0]

    @property
    def name(self):
        name_parts = itertools.chain(
            self.relative_path.parts[1:-1], [self.relative_path.stem]
        )
        return "__".join(name_parts)

    def __repr__(self):
        return f"{self.schema}.{self.name}"

    @classmethod
    def from_path(cls, path, origin):
        relative_path = path.relative_to(origin)
        if path.suffix == ".py":
            return PythonView(origin, relative_path)
        if path.suffix == ".sql":
            return SQLView(origin, relative_path)

    @property
    @abc.abstractmethod
    def dependencies(self) -> set[str]:
        ...


class SQLView(View):
    @property
    def query(self):
        text = self.path.read_text().rstrip().rstrip(";")
        if text.startswith("{% extends"):
            loader = jinja2.FileSystemLoader(self.origin)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(self.relative_path))
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
            else (table.name.split("__")[0], table.name.split("__", 1)[1])
            if "__" in table.name
            else (None, table.name)
            for table in parse.find_all(sqlglot.exp.Table)
        }
        return table_names - cte_names

    @property
    def dependencies(self):
        try:
            return self._parse_dependencies(self.query)
        except sqlglot.errors.ParseError:
            # HACK If SQLGlot can't parse the query, we do it the old-fashioned way
            dependencies = set()
            query = self.query
            for dataset in ["kaya", "niklas", "posthog"]:
                for match in re.finditer(
                    rf"{dataset}\.(?P<view>\w+)", query, re.IGNORECASE
                ):
                    schema, view_name = (
                        match.group("view").split("__", 1)
                        if dataset == "kaya"
                        else (dataset, match.group("view"))
                    )
                    dependencies.add((schema, view_name))
            return dependencies

    @property
    def description(self):
        return " ".join(
            line.lstrip("-").strip()
            for line in itertools.takewhile(
                lambda line: line.startswith("--"), self.query.strip().splitlines()
            )
        )


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


def load_views(views_dir: pathlib.Path | str) -> list[View]:
    if isinstance(views_dir, str):
        views_dir = pathlib.Path(views_dir)
    return [
        View.from_path(path, origin=views_dir)
        for schema_dir in (d for d in views_dir.iterdir() if d.is_dir())
        for path in schema_dir.rglob("*")
        if not path.is_dir()
        and not path.name.startswith("_")
        and path.suffix in {".py", ".sql"}
        and path.stat().st_size > 0
    ]
