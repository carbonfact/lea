from __future__ import annotations

import functools
import io

import pandas as pd

import lea


def get_schema_diff(
    origin_client: lea.clients.Client, target_client: lea.clients.Client
) -> pd.DataFrame:
    origin_columns = set(
        map(tuple, origin_client.get_columns()[["view_name", "column"]].values.tolist())
    )
    destination_columns = set(
        map(tuple, target_client.get_columns()[["view_name", "column"]].values.tolist())
    )

    return pd.DataFrame(
        [
            {
                "view_name": view_name,
                "column": None,
                "diff_kind": "ADDED",
            }
            for view_name in {t for t, _ in origin_columns} - {t for t, _ in destination_columns}
        ]
        + [
            {
                "view_name": view_name,
                "column": column,
                "diff_kind": "ADDED",
            }
            for view_name, column in origin_columns - destination_columns
        ]
        + [
            {
                "view_name": view_name,
                "column": None,
                "diff_kind": "REMOVED",
            }
            for view_name in {t for t, _ in destination_columns} - {t for t, _ in origin_columns}
        ]
        + [
            {
                "view_name": view_name,
                "column": column,
                "diff_kind": "REMOVED",
            }
            for view_name, column in destination_columns - origin_columns
        ]
    )


def get_size_diff(
    origin_client: lea.clients.Client, target_client: lea.clients.Client
) -> pd.DataFrame:
    origin_tables = origin_client.get_tables()[["view_name", "n_rows", "n_bytes"]]
    target_tables = target_client.get_tables()[["view_name", "n_rows", "n_bytes"]]
    comparison = pd.merge(
        origin_tables,
        target_tables,
        on="view_name",
        suffixes=("_origin", "_destination"),
        how="outer",
    ).fillna(0)
    comparison["n_rows_diff"] = comparison.eval("n_rows_origin - n_rows_destination").astype(int)
    comparison["n_bytes_diff"] = comparison.eval("n_bytes_origin - n_bytes_destination").astype(int)
    comparison = comparison[(comparison.n_rows_diff != 0) | (comparison.n_bytes_diff != 0)]
    return comparison


def calculate_diff(origin_client: lea.clients.Client, target_client: lea.clients.Client) -> str:
    schema_diff = get_schema_diff(origin_client=origin_client, target_client=target_client)
    size_diff = get_size_diff(origin_client=origin_client, target_client=target_client)

    if schema_diff.empty and size_diff.empty:
        return "No schema or content change detected."

    removed_view_names = set(
        schema_diff[schema_diff.column.isnull() & (schema_diff.diff_kind == "REMOVED")].view_name
    )
    added_view_names = set(
        schema_diff[schema_diff.column.isnull() & (schema_diff.diff_kind == "ADDED")].view_name
    )
    modified_view_names = set(size_diff.view_name)

    buffer = io.StringIO()
    print_ = functools.partial(print, file=buffer)
    for view_name in removed_view_names | added_view_names | modified_view_names:
        view_schema_diff = schema_diff[
            schema_diff.column.notnull() & schema_diff.view_name.eq(view_name)
        ]
        view_size_diff = size_diff[size_diff.view_name.eq(view_name)].iloc[0]

        if view_name in removed_view_names:
            print_(f"- {view_name}")
        elif view_name in added_view_names:
            print_(f"+ {view_name}")
        else:
            print_(f"  {view_name}")

        if view_name in modified_view_names:
            sign = "+" if view_size_diff.n_rows_diff > 0 else "-"
            print_(f"{sign} {abs(view_size_diff.n_rows_diff):,d} rows")
        for removed in view_schema_diff[view_schema_diff.diff_kind == "REMOVED"].column:
            print_(f"- {removed}")
        for added in view_schema_diff[view_schema_diff.diff_kind == "ADDED"].column:
            print_(f"+ {added}")
        print_()

    return buffer.getvalue().rstrip()
