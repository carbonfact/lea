from __future__ import annotations

import functools
import io

import pandas as pd

import lea


def get_schema_diff(
    origin_client: lea.clients.Client, target_client: lea.clients.Client
) -> pd.DataFrame:
    origin_columns = origin_client.list_columns()[["table_reference", "column"]]
    target_columns = target_client.list_columns()[["table_reference", "column"]]

    # HACK: remove the username
    origin_columns["table_reference"] = origin_columns["table_reference"].apply(
        lambda x: x.split(".", 1)[1]
    )
    target_columns["table_reference"] = target_columns["table_reference"].apply(
        lambda x: x.split(".", 1)[1]
    )

    origin_columns = set(map(tuple, origin_columns.values.tolist()))
    target_columns = set(map(tuple, target_columns.values.tolist()))

    return pd.DataFrame(
        [
            {
                "table_reference": table_reference,
                "column": None,
                "diff_kind": "ADDED",
            }
            for table_reference in {t for t, _ in origin_columns} - {t for t, _ in target_columns}
        ]
        + [
            {
                "table_reference": table_reference,
                "column": column,
                "diff_kind": "ADDED",
            }
            for table_reference, column in origin_columns - target_columns
        ]
        + [
            {
                "table_reference": table_reference,
                "column": None,
                "diff_kind": "REMOVED",
            }
            for table_reference in {t for t, _ in target_columns} - {t for t, _ in origin_columns}
        ]
        + [
            {
                "table_reference": table_reference,
                "column": column,
                "diff_kind": "REMOVED",
            }
            for table_reference, column in target_columns - origin_columns
        ],
        columns=["table_reference", "column", "diff_kind"],
    )


def get_size_diff(
    origin_client: lea.clients.Client, target_client: lea.clients.Client
) -> pd.DataFrame:
    origin_tables = origin_client.list_tables()[["table_reference", "n_rows", "n_bytes"]]
    target_tables = target_client.list_tables()[["table_reference", "n_rows", "n_bytes"]]

    # HACK: remove the username
    origin_tables["table_reference"] = origin_tables["table_reference"].apply(
        lambda x: x.split(".", 1)[1]
    )
    target_tables["table_reference"] = target_tables["table_reference"].apply(
        lambda x: x.split(".", 1)[1]
    )

    comparison = pd.merge(
        origin_tables,
        target_tables,
        on="table_reference",
        suffixes=("_origin", "_destination"),
        how="outer",
    ).fillna(0)
    comparison["n_rows_diff"] = (
        comparison["n_rows_origin"] - comparison["n_rows_destination"]
    ).astype(int)
    comparison["n_bytes_diff"] = (
        comparison["n_bytes_origin"] - comparison["n_bytes_destination"]
    ).astype(int)
    # TODO: include bytes
    comparison = comparison[comparison.n_rows_diff != 0]
    return comparison


def calculate_diff(origin_client: lea.clients.Client, target_client: lea.clients.Client) -> str:
    schema_diff = get_schema_diff(origin_client=origin_client, target_client=target_client)
    size_diff = get_size_diff(origin_client=origin_client, target_client=target_client)

    if schema_diff.empty and size_diff.empty:
        return "No schema or content change detected."

    removed_table_references = set(
        schema_diff[
            schema_diff.column.isnull() & (schema_diff.diff_kind == "REMOVED")
        ].table_reference
    )
    added_table_references = set(
        schema_diff[
            schema_diff.column.isnull() & (schema_diff.diff_kind == "ADDED")
        ].table_reference
    )
    modified_table_references = set(size_diff.table_reference)

    buffer = io.StringIO()
    print_ = functools.partial(print, file=buffer)
    for table_reference in sorted(
        removed_table_references | added_table_references | modified_table_references
    ):
        view_schema_diff = schema_diff[
            schema_diff.column.notnull() & schema_diff.table_reference.eq(table_reference)
        ]
        view_size_diff = size_diff[size_diff.table_reference.eq(table_reference)].iloc[0]

        if table_reference in removed_table_references:
            print_(f"- {table_reference}")
        elif table_reference in added_table_references:
            print_(f"+ {table_reference}")
        elif table_reference in modified_table_references:
            print_(f"  {table_reference}")

        if table_reference in modified_table_references:
            # #rows changed
            if view_size_diff.n_rows_diff:
                sign = "+" if view_size_diff.n_rows_diff > 0 else "-"
                print_(f"{sign} {abs(view_size_diff.n_rows_diff):,d} rows")
            # TODO: #bytes changed
        for removed in sorted(view_schema_diff[view_schema_diff.diff_kind == "REMOVED"].column):
            print_(f"- {removed}")
        for added in sorted(view_schema_diff[view_schema_diff.diff_kind == "ADDED"].column):
            print_(f"+ {added}")
        print_()

    return buffer.getvalue().rstrip()
