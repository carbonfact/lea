from __future__ import annotations

import collections
import dataclasses

import sqlglot

from .dialects import SQLDialect


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


def extract_comments(
    code: str, expected_field_names: list[str], sql_dialect: SQLDialect
) -> dict[str, CommentBlock]:
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
        token
        for token in tokens
        if token.token_type.value == "VAR" and token.text in expected_field_names
    ]

    def is_var_line(line):
        line_tokens = [t for t in tokens if t.line == line and t.token_type.value != "COMMA"]
        return line_tokens[-1].token_type.value == "VAR"

    last_var_per_line = {token.line: token.text for token in var_tokens if is_var_line(token.line)}

    # Now assign each comment block to a variable
    var_comments = {}
    for comment_block in comment_blocks:
        adjacent_var = next(
            (var for line, var in last_var_per_line.items() if comment_block.last_line == line - 1),
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
