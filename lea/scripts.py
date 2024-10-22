from __future__ import annotations

import collections
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
    table_ref: TableRef
    code: str
    sql_dialect: SQLDialect

    @classmethod
    def from_path(cls, dataset_dir: pathlib.Path, relative_path: pathlib.Path, sql_dialect: SQLDialect) -> SQLScript:

        # Either the file is a Jinja template
        if relative_path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(dataset_dir)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(relative_path))
            code = template.render(env=os.environ)
        # Or it's a regular SQL file
        else:
            code = (dataset_dir / relative_path).read_text().rstrip().rstrip(";")

        return cls(
            table_ref=TableRef.from_path(dataset_dir, relative_path),
            code=code,
            sql_dialect=sql_dialect,
        )

    @property
    def is_test(self) -> bool:
        return self.table_ref.schema and self.table_ref.schema[0] == "tests"

    @functools.cached_property
    def ast(self):
        ast = sqlglot.parse_one(self.code, dialect=self.sql_dialect.sqlglot_dialect)
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

    @functools.cached_property
    def fields(self) -> list[Field]:
        try:
            field_names = self.ast.named_selects
        except sqlglot.errors.ParseError:
            field_names = []
        field_comments = extract_comments(code=self.code, expected_field_names=field_names, sql_dialect=self.sql_dialect)
        return [
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
                name=f"{'__'.join(self.table_ref.schema)}__{self.table_ref.name}__{field.name}#{tag}",
            )

        def make_assertion_test(table_ref, field, tag):
            if tag == "#NO_NULLS":
                return SQLScript(
                    table_ref=make_table_ref(field, "no_nulls"),
                    code=self.sql_dialect.make_column_test_no_nulls(table_ref, field.name),
                    sql_dialect=self.sql_dialect,
                )
            elif tag == "#UNIQUE":
                return SQLScript(
                    table_ref=make_table_ref(field, "unique"),
                    code=self.sql_dialect.make_column_test_unique(table_ref, field.name),
                    sql_dialect=self.sql_dialect,
                )
            elif unique_by := re.fullmatch("#UNIQUE_BY" + r"\((?P<by>.+)\)", tag):
                by = unique_by.group("by")
                return SQLScript(
                    table_ref=make_table_ref(field, f"unique_by_{by}"),
                    code=self.sql_dialect.make_column_test_unique_by(table_ref, field.name, by),
                    sql_dialect=self.sql_dialect,
                )
            elif set_ := re.fullmatch("#SET" + r"\{(?P<elements>\w+(?:,\s*\w+)*)\}", tag):
                elements = {element.strip() for element in set_.group("elements").split(",")}
                return SQLScript(
                    table_ref=make_table_ref(field, "set"),
                    code=self.sql_dialect.make_column_test_set(table_ref, field.name, elements),
                    sql_dialect=self.sql_dialect,
                )
            else:
                raise ValueError(f"Unhandled tag: {tag}")

        return [
            make_assertion_test(self.table_ref, field, tag)
            for field in self.fields
            for tag in field.tags
        ]

    # Mmm not sure about the following properties

    def edit_dependencies_dataset(self, dependencies_to_edit: set[TableRef], new_dataset: str) -> SQLScript:
        # TODO: could be done faster with Aho–Corasick algorithm
        # Maybe try out https://github.com/vi3k6i5/flashtext
        code = self.code
        for dependency in self.dependencies:
            if dependencies_to_edit & {dependency.replace_dataset(None), dependency}:
                dependency_str = self.sql_dialect.table_ref_to_str(dependency)
                code = re.sub(rf"\b{dependency_str}\b", dependency.replace_dataset(new_dataset), code)
        return dataclasses.replace(
            self,
            code=code
        )

    def replace_dataset(self, dataset: str) -> SQLScript:
        return dataclasses.replace(
            self,
            table_ref=self.table_ref.replace_dataset(dataset)
        )



@dataclasses.dataclass
class Field:
    name: str
    tags: set[str]
    description: str

    @property
    def is_unique(self):
        return "#UNIQUE" in self.tags


@dataclasses.dataclass
class Comment:
    line: int
    text: str


class CommentBlock(collections.UserList):
    def __init__(self, comments: list[Comment]):
        super().__init__(sorted(comments, key=lambda c: c.line))

    @property
    def first_line(self):
        return self[0].line

    @property
    def last_line(self):
        return self[-1].line


def extract_comments(code: str, expected_field_names: list[str], sql_dialect: SQLDialect) -> dict[str, CommentBlock]:

    dialect = sqlglot.Dialect.get_or_raise(sql_dialect.sqlglot_dialect.value)
    tokens = dialect.tokenizer_class().tokenize(code)

    # Extract comments, which are lines that start with --
    comments = [
        Comment(line=line, text=comment.replace("--", "").strip())
        for line, comment in enumerate(code.splitlines(), start=1)
        if comment.strip().startswith("--")
    ]

    # Pack comments into CommentBlock objects
    comment_blocks = merge_adjacent_comments(comments)

    # We assume the tokens are stored. Therefore, by looping over them and building a dictionary,
    # each key will be unique and the last value will be the last variable in the line.
    var_tokens = [
        token for token in tokens if token.token_type.value == "VAR" and token.text in expected_field_names
    ]

    def is_var_line(line):
        line_tokens = [t for t in tokens if t.line == line and t.token_type.value != "COMMA"]
        return line_tokens[-1].token_type.value == "VAR"

    last_var_per_line = {
        token.line: token.text for token in var_tokens if is_var_line(token.line)
    }

    # Now assign each comment block to a variable
    var_comments = {}
    for comment_block in comment_blocks:
        adjacent_var = next(
            (
                var
                for line, var in last_var_per_line.items()
                if comment_block.last_line == line - 1
            ),
            None,
        )
        if adjacent_var:
            var_comments[adjacent_var] = comment_block

    return var_comments


def merge_adjacent_comments(comments: list[Comment]) -> list[CommentBlock]:
    if not comments:
        return []

    # Sort comments by their line number
    comments.sort(key=lambda c: c.line)

    merged_blocks = []
    current_block = [comments[0]]

    # Iterate through comments and group adjacent ones
    for i in range(1, len(comments)):
        if comments[i].line == comments[i - 1].line + 1:  # Check if adjacent
            current_block.append(comments[i])
        else:
            # Create a CommentBlock for the current group
            merged_blocks.append(CommentBlock(current_block))
            # Start a new block
            current_block = [comments[i]]

    # Add the last block
    merged_blocks.append(CommentBlock(current_block))

    return merged_blocks


Script = SQLScript


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
