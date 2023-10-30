from __future__ import annotations

import collections
import dataclasses
import itertools
import os
import re
import textwrap
import warnings

import jinja2
import sqlglot
import sqlglot.optimizer.scope

import lea

from .base import View


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
class SQLView(View):
    sqlglot_dialect: sqlglot.Dialect

    def __repr__(self):
        return ".".join(self.key)

    @property
    def query(self):
        text = self.path.read_text().rstrip().rstrip(";")
        if self.path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(self.origin)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(self.relative_path))
            return template.render(env=os.environ)
        return text

    def parse_dependencies(self, query) -> set[tuple[str, str]]:
        expression = sqlglot.parse_one(query, dialect=self.sqlglot_dialect)
        dependencies = set()

        for scope in sqlglot.optimizer.scope.traverse_scope(expression):
            for table in scope.tables:
                if (
                    not isinstance(table.this, sqlglot.exp.Func)
                    and sqlglot.exp.table_name(table) not in scope.cte_sources
                ):
                    if self.sqlglot_dialect is sqlglot.dialects.Dialects.BIGQUERY:
                        dependencies.add(tuple(table.name.split(lea._SEP)))
                    elif self.sqlglot_dialect is sqlglot.dialects.Dialects.DUCKDB:
                        dependencies.add((table.db, *table.name.split(lea._SEP)))
                    else:
                        raise ValueError(f"Unsupported SQL dialect: {self.sqlglot_dialect}")

        return dependencies

    @property
    def dependencies(self):
        try:
            return self.parse_dependencies(self.query)
        except sqlglot.errors.ParseError:
            warnings.warn(
                f"SQLGlot couldn't parse {self.path} with dialect {self.sqlglot_dialect}. Falling back to regex."
            )
            dependencies = set()
            for match in re.finditer(
                r"(JOIN|FROM)\s+(?P<schema>[a-z][a-z_]+[a-z])\.(?P<view>[a-z][a-z_]+[a-z])",
                self.query,
                re.IGNORECASE,
            ):
                schema, view_name = (
                    (
                        match.group("view").split(lea._SEP)[0],
                        match.group("view").split("__", 1)[1],
                    )
                    if "__" in match.group("view")
                    else (match.group("schema"), match.group("view"))
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

    def extract_comments(self, columns: list[str]) -> dict[str, CommentBlock]:
        dialect = sqlglot.Dialect.get_or_raise(self.sqlglot_dialect)()
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
                    (cb for cb in comment_blocks if cb.first_line == comment_block.last_line + 1),
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
            token for token in tokens if token.token_type.value == "VAR" and token.text in columns
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


class GenericSQLView(SQLView):
    def __init__(self, schema, name, query, sqlglot_dialect):
        self._schema = schema
        self._name = name
        self._query = textwrap.dedent(query)
        self._sqlglot_dialect = sqlglot_dialect

    @property
    def key(self):
        return (self._schema, self._name)

    @property
    def query(self):
        return self._query

    @property
    def sqlglot_dialect(self):
        return self._sqlglot_dialect
