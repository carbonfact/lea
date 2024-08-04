from __future__ import annotations

import collections
import dataclasses
import functools
import itertools
import os
import re
import warnings

import jinja2
import rich.syntax
import sqlglot
import sqlglot.optimizer.qualify
import sqlglot.optimizer.scope

import lea

from .base import Field, View


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
    @property
    def sqlglot_dialect(self):
        return self.client.sqlglot_dialect

    def __repr__(self):
        return ".".join(self.key)

    @classmethod
    def path_suffixes(self):
        return {"sql", "sql.jinja"}

    @functools.cached_property
    def query(self):
        # Handle Jinja files
        if self.path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(self.origin)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(self.relative_path))
            return template.render(env=os.environ)
        # Handle regular SQL files
        return self.path.read_text().rstrip().rstrip(";")

    @functools.cached_property
    def ast(self):
        ast = sqlglot.parse_one(self.query, dialect=self.sqlglot_dialect)
        try:
            return sqlglot.optimizer.qualify.qualify(ast)
        except sqlglot.errors.OptimizeError:
            return ast

    @property
    def fields(self):
        if hasattr(self, "_fields"):
            return self._fields
        try:
            field_names = self.ast.named_selects
        except sqlglot.errors.ParseError:
            field_names = []
        field_comments = self.extract_comments(field_names)
        self._fields = [
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
        return self._fields

    @functools.cached_property
    def dependent_view_keys(self):
        table_references = set()
        try:
            for scope in sqlglot.optimizer.scope.traverse_scope(self.ast):
                for table in scope.tables:
                    if (
                        not isinstance(table.this, sqlglot.exp.Func)
                        and sqlglot.exp.table_name(table) not in scope.cte_sources
                    ):
                        table_references.add(sqlglot.exp.table_name(table))
        except sqlglot.errors.ParseError:
            warnings.warn(
                f"SQLGlot couldn't parse {repr(self)} with dialect {self.sqlglot_dialect}. Falling back to regex."
            )
            for match in re.finditer(
                r"(JOIN|FROM)\s+(?P<schema>[a-z][a-z_\.]+[a-z])\.(?P<view>[a-z][a-z_]+[a-z])",
                self.query,
                re.IGNORECASE,
            ):
                table_reference = f"{match.group('schema')}.{match.group('view')}"
                table_references.add(table_reference)

        return {
            self.client._table_reference_to_view_key(table_reference)
            for table_reference in table_references
        }

    @functools.cached_property
    def description(self):
        return " ".join(
            line.lstrip("-").strip()
            for line in itertools.takewhile(
                lambda line: line.startswith("--"), self.query.strip().splitlines()
            )
        )

    def extract_comments(self, columns: list[str]) -> dict[str, CommentBlock]:
        if not columns:
            return {}

        dialect = sqlglot.Dialect.get_or_raise(self.sqlglot_dialect.value)
        tokens = dialect.tokenizer_class().tokenize(self.query)

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

    def with_context(self, table_reference_mapping: dict[str, str]):
        query = self.query
        # TODO: could be done faster with Aho–Corasick algorithm
        # Maybe try out https://github.com/vi3k6i5/flashtext
        for k, v in table_reference_mapping.items():
            query = re.sub(rf"\b{k}\b", v, query)
        view = InMemorySQLView(
            key=self.key,
            query=query,
            client=self.client,
        )
        view._fields = self.fields  # HACK
        return view

    def __rich__(self):
        return rich.syntax.Syntax(self.query, "sql")


class InMemorySQLView(SQLView):
    def __init__(
        self,
        key: tuple[str, ...],
        query: str,
        client: lea.clients.base.Client,
        origin: InMemorySQLView | None = None,
    ):
        self._key = key
        self._query = query
        self.client = client
        self.origin = origin

    @property
    def key(self):
        return self._key

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, new_query):
        self._query = new_query
