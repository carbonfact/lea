from __future__ import annotations

import dataclasses
import functools
import pathlib
import os
import re

import jinja2
import sqlglot
import sqlglot.optimizer

from .table_ref import TableRef
from .dialects import SQLDialect


@dataclasses.dataclass(frozen=True)
class SQLScript:
    dataset_dir: pathlib.Path
    relative_path: pathlib.Path
    table_ref: TableRef
    sql: str
    sql_dialect: SQLDialect

    @classmethod
    def from_path(cls, dataset_dir: pathlib.Path, relative_path: pathlib.Path, sql_dialect: SQLDialect) -> SQLScript:

        # Either the file is a Jinja template
        if relative_path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(dataset_dir)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(relative_path))
            sql = template.render(env=os.environ)
        # Or it's a regular SQL file
        else:
            sql = (dataset_dir / relative_path).read_text().rstrip().rstrip(";")

        return cls(
            dataset_dir=dataset_dir,
            relative_path=relative_path,
            table_ref=TableRef.from_path(dataset_dir, relative_path),
            sql=sql,
            sql_dialect=sql_dialect,
        )

    @property
    def is_test(self) -> bool:
        return self.table_ref.schema and self.table_ref.schema[0] == "tests"

    @functools.cached_property
    def ast(self):
        ast = sqlglot.parse_one(self.sql, dialect=self.sql_dialect.sqlglot_dialect)
        try:
            return sqlglot.optimizer.qualify.qualify(ast)
        except sqlglot.errors.OptimizeError:
            return ast

    @functools.cached_property
    def dependencies(self) -> set[TableRef]:
        return {
            self.sql_dialect.parse_table_ref(table_ref=sqlglot.exp.table_name(table))
            for scope in sqlglot.optimizer.scope.traverse_scope(self.ast)
            for table in scope.tables
            if (
                not isinstance(table.this, sqlglot.exp.Func)
                and sqlglot.exp.table_name(table) not in scope.cte_sources
            )
        }

    def edit_dependencies_dataset(self, dependencies_to_edit: set[TableRef], new_dataset: str) -> SQLScript:
        # TODO: could be done faster with Aho–Corasick algorithm
        # Maybe try out https://github.com/vi3k6i5/flashtext
        sql = self.sql
        for dependency in self.dependencies:
            if dependencies_to_edit & {dependency.replace_dataset(None), dependency}:
                dependency_str = self.sql_dialect.table_ref_to_str(dependency)
                sql = re.sub(rf"\b{dependency_str}\b", dependency.replace_dataset(new_dataset), sql)
        return dataclasses.replace(
            self,
            sql=sql
        )

    def replace_dataset(self, dataset: str) -> SQLScript:
        return dataclasses.replace(
            self,
            table_ref=self.table_ref.replace_dataset(dataset)
        )



@dataclasses.dataclass(frozen=True)
class JSONScript:
    table_ref: TableRef

    @classmethod
    def from_path(cls, path: pathlib.Path) -> JSONScript:
        return cls(table_ref=TableRef.from_path(path))

    @property
    def dependencies(self) -> set[TableRef]:
        return set()



Script = SQLScript | JSONScript


def read_scripts(dataset_dir: pathlib.Path, sql_dialect: SQLDialect) -> list[Script]:

    def read_script(path: pathlib.Path) -> Script:
        match tuple(path.suffixes):
            case (".sql",) | (".sql", ".jinja"):
                # 🐉
                # SQL scripts may include the dataset when they reference tables. We want to determine
                # dependencies between scripts. Therefore, we are not interested in the dataset of the
                # dependencies. We know what the target dataset is called, so we can remove it from the
                # dependencies.
                return SQLScript.from_path(dataset_dir=dataset_dir, relative_path=path.relative_to(dataset_dir), sql_dialect=sql_dialect)
            # case (".json",):
            #     return JSONScript.from_path(path.relative_to(directory))
            case _:
                raise ValueError(f"Unsupported script type: {path}")

    return [
        read_script(path)
        for path in dataset_dir.rglob("*")
        if not path.is_dir()
        and tuple(path.suffixes) in {(".sql",), (".sql", ".jinja"), (".json",)}
        and not path.name.startswith("_")
        and path.stat().st_size > 0
    ]
