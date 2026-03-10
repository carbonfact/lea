from __future__ import annotations

import dataclasses
import datetime as dt
import functools
import os
import pathlib
import pickle
import re
import textwrap

import jinja2
import rich.syntax
import sqlglot
import sqlglot.errors
import sqlglot.expressions
import sqlglot.optimizer
import sqlglot.optimizer.qualify
import sqlglot.optimizer.scope
import yaml

from .comment import extract_comments
from .dialects import SQLDialect
from .field import Field, FieldTag
from .table_ref import TableRef


@dataclasses.dataclass(frozen=True)
class SQLScript:
    table_ref: TableRef
    code: str
    sql_dialect: SQLDialect
    fields: list[Field] | None = dataclasses.field(default=None)
    updated_at: dt.datetime | None = None
    _cached_dependencies: set[TableRef] | None = dataclasses.field(default=None, repr=False)

    def __post_init__(self):
        """

        This part is a bit tricky. We extract fields from each script for different reasons. For
        instance, the fields are used to generate assertion tests.

        The logic to extract fields is based on SQLGlot. The latter usually works well, but it
        sometimes fail for complex queries. For instance, in incremental mode, we have to edit
        the queries to filter their dependencies. These queries are not always parsed correctly by
        SQLGlot.

        To circumvent this issue, we extract fields, and cache them. This way, whenever we call
        dataclasses.replace, they won't have to be recomputed. This makes sense because the scripts
        are never edited to add or remove fields. They are only edited to change the filtering
        conditions.

        """

        if self.fields is not None:
            return
        if isinstance(self.ast, sqlglot.exp.Command):
            fields = []
        else:
            field_names = self.ast.named_selects
            field_comments = extract_comments(
                code=self.code,
                expected_field_names=field_names,
                sql_dialect=self.sql_dialect,
            )
            fields = [
                Field(
                    name=name,
                    tags={
                        comment.text
                        for comment in field_comments.get(name, [])
                        if comment.text.startswith("#")
                    },
                    description=" ".join(
                        comment.text
                        for comment in field_comments.get(name, [])
                        if not comment.text.startswith("#")
                    ),
                )
                for name in field_names
                if name != "*"
            ]
        # https://stackoverflow.com/a/54119384
        object.__setattr__(self, "fields", fields)

    @classmethod
    def from_path(
        cls,
        scripts_dir: pathlib.Path,
        relative_path: pathlib.Path,
        sql_dialect: SQLDialect,
        project_name: str | None,
        fields: list[Field] | None = None,
        cached_dependencies: set[TableRef] | None = None,
    ) -> SQLScript:
        # Either the file is a Jinja template
        if relative_path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(scripts_dir)
            environment = jinja2.Environment(loader=loader)

            def load_yaml(path: str) -> dict:
                full_path = (scripts_dir / path).resolve()
                project_root = scripts_dir.resolve().parent
                if not full_path.is_relative_to(project_root):
                    raise ValueError(f"load_yaml path escapes project root: {path}")
                with open(full_path) as f:
                    return yaml.safe_load(f)

            environment.globals["load_yaml"] = load_yaml  # ty: ignore[invalid-assignment]
            template = environment.get_template(str(relative_path))
            code = template.render(env=os.environ, load_yaml=load_yaml)
        # Or it's a regular SQL file
        else:
            code = (scripts_dir / relative_path).read_text().rstrip().rstrip(";")

        return cls(
            table_ref=TableRef.from_path(
                scripts_dir=scripts_dir,
                relative_path=relative_path,
                project_name=project_name,
            ),
            code=code,
            sql_dialect=sql_dialect,
            fields=fields,
            _cached_dependencies=cached_dependencies,
            updated_at=dt.datetime.fromtimestamp(
                (scripts_dir / relative_path).stat().st_mtime, tz=dt.UTC
            ),
        )

    @property
    def is_test(self) -> bool:
        return self.table_ref.is_test

    @functools.cached_property
    def expressions(self) -> list[sqlglot.Expression]:
        return list(
            filter(
                None,
                sqlglot.parse(self.code, dialect=self.sql_dialect.sqlglot_dialect),
            )
        )

    @property
    def header_statements(self) -> list[str]:
        return (
            [
                expr.sql(dialect=self.sql_dialect.sqlglot_dialect, pretty=True)
                for expr in self.expressions[:-1]
            ]
            if len(self.expressions) > 1
            else []
        )

    @property
    def query(self) -> str:
        return self.expressions[-1].sql(dialect=self.sql_dialect.sqlglot_dialect, pretty=True)

    @functools.cached_property
    def ast(self):
        ast = self.expressions[-1]
        try:
            return sqlglot.optimizer.qualify.qualify(ast)
        except sqlglot.errors.OptimizeError:
            return ast

    @functools.cached_property
    def dependencies(self) -> set[TableRef]:
        if self._cached_dependencies is not None:
            return self._cached_dependencies

        def add_default_project(table_ref: TableRef) -> TableRef:
            if table_ref.project is None:
                return table_ref.replace_project(self.table_ref.project)
            return table_ref

        dependencies = set()

        for expression in self.expressions:
            for table_name in find_table_names(expression):
                try:
                    table_ref = self.sql_dialect.parse_table_ref(table_ref=table_name)
                except ValueError as e:
                    raise ValueError(
                        f"Unable to parse table reference {table_name!r} "
                        f"in {self.table_ref.replace_project(None)}"
                    ) from e
                dependencies.add(add_default_project(table_ref))

        return dependencies

    @property
    def assertion_tests(self) -> list[SQLScript]:
        """

        Assertion tests are gleaned from the comments in the script. They are used to test the
        quality of the data. The following tags are supported:

        - #NO_NULLS: Asserts that the column has no null values.
        - #UNIQUE: Asserts that the column has unique values.
        - #UNIQUE_BY(field): Asserts that the column has unique values when grouped by field.
        - #SET{value1, value2, ...}: Asserts that the column only contains the specified elements.

        """

        def make_table_ref(field, tag):
            return TableRef(
                dataset=self.table_ref.dataset,
                schema=("tests",),
                name=f"{'__'.join(self.table_ref.schema)}__{self.table_ref.name}__{field.name}___{tag.lower().lstrip('#')}",
                project=self.table_ref.project,
            )

        def make_assertion_test(table_ref, field, tag):
            if tag == FieldTag.NO_NULLS:
                return SQLScript(
                    table_ref=make_table_ref(field, FieldTag.NO_NULLS),
                    code=self.sql_dialect.make_column_test_no_nulls(table_ref, field.name),
                    sql_dialect=self.sql_dialect,
                )
            elif tag == FieldTag.UNIQUE:
                return SQLScript(
                    table_ref=make_table_ref(field, FieldTag.UNIQUE),
                    code=self.sql_dialect.make_column_test_unique(table_ref, field.name),
                    sql_dialect=self.sql_dialect,
                )
            elif unique_by := re.fullmatch(FieldTag.UNIQUE_BY + r"\((?P<by>.+)\)", tag):
                by = unique_by.group("by")
                return SQLScript(
                    table_ref=make_table_ref(field, FieldTag.UNIQUE_BY),
                    code=self.sql_dialect.make_column_test_unique_by(table_ref, field.name, by),
                    sql_dialect=self.sql_dialect,
                )
            elif set_ := re.fullmatch(
                FieldTag.SET + r"\{(?P<elements>'[^']+'(?:,\s*'[^']+')*)\}", tag
            ):
                elements = {element.strip() for element in set_.group("elements").split(",")}
                return SQLScript(
                    table_ref=make_table_ref(field, FieldTag.SET),
                    code=self.sql_dialect.make_column_test_set(table_ref, field.name, elements),
                    sql_dialect=self.sql_dialect,
                )
            else:
                raise ValueError(f"Unhandled tag: {tag}")

        return [
            # We don't need to include the target table_ref's project in the assertion test,
            # because that would include the project in the code generated by the SQL dialect.
            # This is not needed, because the project will be set downstream in each script anyway.
            make_assertion_test(self.table_ref.replace_project(None), field, tag)
            for field in self.fields or []
            for tag in field.tags
            if tag not in {FieldTag.INCREMENTAL, FieldTag.CLUSTERING_FIELD}
        ]

    def replace_table_ref(self, table_ref: TableRef) -> SQLScript:
        return dataclasses.replace(self, table_ref=table_ref)

    def __rich__(self):
        code = textwrap.dedent(self.code).strip()
        code_with_table_ref = f"""-- {self.table_ref}\n\n{code}\n"""
        return rich.syntax.Syntax(code_with_table_ref, "sql", line_numbers=False, theme="ansi_dark")


Script = SQLScript


_CACHE_VERSION = f"1_{sqlglot.__version__}"  # ty: ignore[possibly-missing-attribute]


def _env_hash() -> str:
    """Hash all LEA_* environment variables to detect config changes."""
    import hashlib

    lea_vars = sorted(
        (k, v) for k, v in os.environ.items() if k.startswith("LEA_")
    )
    return hashlib.sha256(str(lea_vars).encode()).hexdigest()


def _load_cache(cache_path: pathlib.Path) -> dict:
    """Load the script cache. Returns {relative_path_str: entry}."""
    try:
        with open(cache_path, "rb") as f:
            data = pickle.load(f)
        if data.get("version") == _CACHE_VERSION and data.get("env_hash") == _env_hash():
            return data.get("scripts", {})
    except (FileNotFoundError, pickle.UnpicklingError, KeyError, EOFError):
        pass
    return {}


def _save_cache(cache_path: pathlib.Path, entries: dict) -> None:
    """Save the script cache."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump({"version": _CACHE_VERSION, "env_hash": _env_hash(), "scripts": entries}, f)


def read_scripts(
    scripts_dir: pathlib.Path,
    sql_dialect: SQLDialect,
    dataset_name: str,
    project_name: str | None,
    cache_dir: pathlib.Path | None = None,
) -> list[Script]:
    # Load existing cache
    cache_path = cache_dir / "cache.pkl" if cache_dir else None
    cache_entries = _load_cache(cache_path) if cache_path else {}
    cache_dirty = False

    def read_script(path: pathlib.Path) -> Script:
        nonlocal cache_dirty

        match tuple(path.suffixes):
            case (".sql",) | (".sql", ".jinja"):
                relative_path = path.relative_to(scripts_dir)
                mtime = path.stat().st_mtime
                cache_key = str(relative_path)

                # Try cache — on hit, we still read the file content but skip
                # expensive parsing (sqlglot.parse, qualify, traverse_scope)
                cached = None
                entry = cache_entries.get(cache_key)
                if entry and entry.get("mtime") == mtime:
                    cached = entry

                script = SQLScript.from_path(
                    scripts_dir=scripts_dir,
                    relative_path=relative_path,
                    sql_dialect=sql_dialect,
                    project_name=project_name,
                    fields=cached["fields"] if cached else None,
                    cached_dependencies=cached["dependencies"] if cached else None,
                )

                # Update cache on miss
                if cache_path and cached is None:
                    cache_entries[cache_key] = {
                        "mtime": mtime,
                        "fields": script.fields,
                        "dependencies": script.dependencies,
                    }
                    cache_dirty = True

                return script
            case _:
                raise ValueError(f"Unsupported script type: {path}")

    def set_dataset(script: Script) -> Script:
        return script.replace_table_ref(script.table_ref.replace_dataset(dataset=dataset_name))

    scripts = [
        set_dataset(read_script(path))
        for path in scripts_dir.rglob("*")
        if not path.is_dir()
        and tuple(path.suffixes) in {(".sql",), (".sql", ".jinja"), (".json",)}
        and not path.name.startswith("_")
        and path.stat().st_size > 0
    ]

    # Write cache if anything changed
    if cache_dirty:
        _save_cache(cache_path, cache_entries)

    return scripts


def find_table_names(expression: sqlglot.Expression) -> set[str]:
    if isinstance(expression, sqlglot.expressions.Set | sqlglot.expressions.Declare):
        return find_table_names_using_find_all(expression)
    return find_table_names_using_find_all_in_scope(expression)


def find_table_names_using_find_all(expression: sqlglot.Expression) -> set[str]:
    return {
        sqlglot.expressions.table_name(e)
        for e in expression.walk()
        if isinstance(e, sqlglot.expressions.Table)
    } - {e.alias for e in expression.walk() if isinstance(e, sqlglot.expressions.CTE)}


def find_table_names_using_find_all_in_scope(
    expression: sqlglot.Expression,
) -> set[str]:
    return {
        sqlglot.expressions.table_name(table)
        for scope in sqlglot.optimizer.scope.traverse_scope(expression)
        for table in scope.tables or []
        if (
            not isinstance(table.this, sqlglot.expressions.Func)
            and sqlglot.expressions.table_name(table) not in scope.cte_sources
        )
    }
