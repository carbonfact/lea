from __future__ import annotations

import dataclasses
import datetime as dt
import functools
import os
import pathlib
import re
import textwrap

import jinja2
import rich.syntax
import sqlglot
import sqlglot.optimizer

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
        field_names = self.ast.named_selects
        field_comments = extract_comments(
            code=self.code, expected_field_names=field_names, sql_dialect=self.sql_dialect
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
        project_name: str,
    ) -> SQLScript:
        # Either the file is a Jinja template
        if relative_path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(scripts_dir)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(relative_path))
            code = template.render(env=os.environ)
        # Or it's a regular SQL file
        else:
            code = (scripts_dir / relative_path).read_text().rstrip().rstrip(";")

        return cls(
            table_ref=TableRef.from_path(
                scripts_dir=scripts_dir, relative_path=relative_path, project_name=project_name
            ),
            code=code,
            sql_dialect=sql_dialect,
            updated_at=dt.datetime.fromtimestamp(
                (scripts_dir / relative_path).stat().st_mtime, tz=dt.timezone.utc
            ),
        )

    @property
    def is_test(self) -> bool:
        return self.table_ref.is_test

    @functools.cached_property
    def ast(self):
        ast = sqlglot.parse_one(self.code, dialect=self.sql_dialect.sqlglot_dialect)
        try:
            return sqlglot.optimizer.qualify.qualify(ast)
        except sqlglot.errors.OptimizeError:
            return ast

    @functools.cached_property
    def dependencies(self) -> set[TableRef]:
        def add_default_project(table_ref: TableRef) -> TableRef:
            if table_ref.project is None:
                return table_ref.replace_project(self.table_ref.project)
            return table_ref

        return {
            add_default_project(
                self.sql_dialect.parse_table_ref(table_ref=sqlglot.exp.table_name(table))
            )
            for scope in sqlglot.optimizer.scope.traverse_scope(self.ast)
            for table in scope.tables
            if (
                not isinstance(table.this, sqlglot.exp.Func)
                and sqlglot.exp.table_name(table) not in scope.cte_sources
            )
        }

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
            elif set_ := re.fullmatch(FieldTag.SET + r"\{(?P<elements>\w+(?:,\s*\w+)*)\}", tag):
                elements = {element.strip() for element in set_.group("elements").split(",")}
                return SQLScript(
                    table_ref=make_table_ref(field, FieldTag.SET),
                    code=self.sql_dialect.make_column_test_set(table_ref, field.name, elements),
                    sql_dialect=self.sql_dialect,
                )
            else:
                raise ValueError(f"Unhandled tag: {tag}")

        return [
            make_assertion_test(self.table_ref, field, tag)
            for field in self.fields or []
            for tag in field.tags
            if tag not in {FieldTag.INCREMENTAL}
        ]

    def replace_table_ref(self, table_ref: TableRef) -> SQLScript:
        return dataclasses.replace(self, table_ref=table_ref)

    def __rich__(self):
        code = textwrap.dedent(self.code).strip()
        code_with_table_ref = f"""-- {self.table_ref}\n\n{code}\n"""
        return rich.syntax.Syntax(code_with_table_ref, "sql", line_numbers=False, theme="ansi_dark")


Script = SQLScript


def read_scripts(
    scripts_dir: pathlib.Path, sql_dialect: SQLDialect, dataset_name: str, project_name: str
) -> list[Script]:
    def read_script(path: pathlib.Path) -> Script:
        match tuple(path.suffixes):
            case (".sql",) | (".sql", ".jinja"):
                return SQLScript.from_path(
                    scripts_dir=scripts_dir,
                    relative_path=path.relative_to(scripts_dir),
                    sql_dialect=sql_dialect,
                    project_name=project_name,
                )
            case _:
                raise ValueError(f"Unsupported script type: {path}")

    def set_dataset(script: Script) -> Script:
        return script.replace_table_ref(script.table_ref.replace_dataset(dataset=dataset_name))

    return [
        set_dataset(read_script(path))
        for path in scripts_dir.rglob("*")
        if not path.is_dir()
        and tuple(path.suffixes) in {(".sql",), (".sql", ".jinja"), (".json",)}
        and not path.name.startswith("_")
        and path.stat().st_size > 0
    ]
