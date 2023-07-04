import functools
import datetime as dt
import pathlib
import time

import concurrent.futures

import lea
import rich.console

def calculate_diff(origin: str, destination: str, client: lea.clients.Client) -> str:

    diff_table = client.get_diff_summary(
        origin_dataset=origin, destination_dataset=destination
    )
    if diff_table.empty:
        return "No field additions or removals detected"

    removed_tables = set(
        diff_table[
            diff_table.column_name.isnull() & (diff_table.diff_kind == "REMOVED")
        ].table_name
    )
    added_tables = set(
        diff_table[
            diff_table.column_name.isnull() & (diff_table.diff_kind == "ADDED")
        ].table_name
    )

    buffer = io.StringIO()
    print_ = functools.partial(print, file=buffer)
    for table, columns in diff_table[diff_table.column_name.notnull()].groupby(
        "table_name"
    ):
        if table in removed_tables:
            print_(f"- {table}")
        elif table in added_tables:
            print_(f"+ {table}")
        else:
            print_(f"  {table}")
        for removed in columns[columns.diff_kind == "REMOVED"].column_name:
            print_(f"- {removed}")
        for added in columns[columns.diff_kind == "ADDED"].column_name:
            print_(f"+ {added}")
        print_()

    return buffer.getvalue()
