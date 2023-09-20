from __future__ import annotations

import collections
import dataclasses
import itertools
import os

import jinja2
import sqlglot

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

class SQLView(View):

    @property
    def query(self):
        text = self.path.read_text().rstrip().rstrip(";")
        if self.path.suffixes == [".sql", ".jinja"]:
            loader = jinja2.FileSystemLoader(self.origin)
            environment = jinja2.Environment(loader=loader)
            template = environment.get_template(str(self.relative_path))
            return template.render(
                env=os.environ
            )
        return text

    @classmethod
    def parse_dependencies(cls, query):
        parse = sqlglot.parse_one(query)  # TODO: allow providing dialect?
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
        return self.parse_dependencies(self.query)

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
