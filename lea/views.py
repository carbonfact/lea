from __future__ import annotations

import abc
import ast
import collections
import dataclasses
import itertools
import os
import pathlib
import re

import jinja2
import sqlglot


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
            self.relative_path.parts[1:-1], [self.relative_path.name.split(".")[0]]
        )
        return "__".join(name_parts)

    @property
    def dunder_name(self):
        return f"{self.schema}__{self.name}"

    def __repr__(self):
        return f"{self.schema}.{self.name}"

    @classmethod
    def from_path(cls, path, origin):
        relative_path = path.relative_to(origin)
        if path.suffix == ".py":
            return PythonView(origin, relative_path)
        if path.suffix == ".sql" or path.suffixes == [".sql", ".jinja"]:
            return SQLView(origin, relative_path)

    @property
    @abc.abstractmethod
    def dependencies(self) -> set[str]:
        ...


class SQLView(View):
    @property
    def query(self):
        text = self.path.read_text().rstrip().rstrip(";")
        if self.path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(self.origin)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(self.relative_path))
            print(os.environ.get("FREEZE_RELEASES", "false"), os.environ.get("FREEZE_RELEASES", "false").lower() == "true")
            return template.render(
                freeze_releases=os.environ.get("FREEZE_RELEASES", "false").lower() == "true"
            )
        return text

    @classmethod
    def _parse_dependencies(cls, sql):
        parse = sqlglot.parse_one(sql)
        cte_names = {(None, cte.alias) for cte in parse.find_all(sqlglot.exp.CTE)}
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

    def extract_comments(
        self, columns: list[str], dialect: str
    ) -> dict[str, CommentBlock]:
        # TODO: we shouldn't have to pass comments

        dialect = sqlglot.Dialect.get_or_raise(dialect)()
        tokens = dialect.tokenizer.tokenize(self.query)

        # Extract comments, which are lines that start with --
        comments = [
            Comment(line=line, text=comment.replace("--", "").strip())
            for line, comment in enumerate(self.query.splitlines(), start=1)
            if comment.strip().startswith("--")
        ]

        # Pack comments into CommentBlock objects
        comment_blocks = [CommentBlock([comment]) for comment in comments]
        comment_blocks = sorted(comment_blocks, key=lambda cb: cb.first_line)

        change = True
        while change:
            change = False
            for comment_block in comment_blocks:
                next_comment_block = next(
                    (
                        cb
                        for cb in comment_blocks
                        if cb.first_line == comment_block.last_line + 1
                    ),
                    None,
                )
                if next_comment_block:
                    comment_block.extend(next_comment_block)
                    next_comment_block.clear()
                    comment_blocks = [cb for cb in comment_blocks if cb]
                    change = True
                    break

        # We assume the tokens are stored. Therefore, by looping over them and building a dictionary,
        # each key will be unique and the last value will be the last variable in the line.
        var_tokens = [
            token
            for token in tokens
            if token.token_type.value == "VAR" and token.text in columns
        ]

        def is_var_line(line):
            line_tokens = [
                t for t in tokens if t.line == line and t.token_type.value != "COMMA"
            ]
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
        and (path.suffix in {".py", ".sql"} or path.suffixes == [".sql", ".jinja"])
        and path.stat().st_size > 0
    ]
