from __future__ import annotations

import functools
import io

import lea


def calculate_diff(origin: str, destination: str, client: lea.clients.Client) -> str:
    diff_table = client.get_diff_summary(origin=origin, destination=destination)
    if diff_table.empty:
        return "No field additions or removals detected"

    removed_tables = set(
        diff_table[diff_table.column.isnull() & (diff_table.diff_kind == "REMOVED")].table
    )
    added_tables = set(
        diff_table[diff_table.column.isnull() & (diff_table.diff_kind == "ADDED")].table
    )

    buffer = io.StringIO()
    print_ = functools.partial(print, file=buffer)
    for table, columns in diff_table[diff_table.column.notnull()].groupby("table"):
        if table in removed_tables:
            print_(f"- {table}")
        elif table in added_tables:
            print_(f"+ {table}")
        else:
            print_(f"  {table}")
        for removed in columns[columns.diff_kind == "REMOVED"].column:
            print_(f"- {removed}")
        for added in columns[columns.diff_kind == "ADDED"].column:
            print_(f"+ {added}")
        print_()

    return buffer.getvalue().rstrip()
