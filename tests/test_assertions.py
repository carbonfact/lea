from __future__ import annotations

import pathlib
import tempfile

import pandas as pd
import pytest

import lea


@pytest.fixture
def client():
    from lea.clients import duckdb

    return duckdb.DuckDB(":memory:", username=None)


@pytest.mark.parametrize(
    "test_data,query,ok",
    [
        pytest.param(*case, id=f"test_assertion{tag}#{i}")
        for tag, cases in {
            "#UNIQUE": [
                (
                    pd.DataFrame({"test_column": [1, 2, 3, 4, 5]}),
                    """
                    SELECT
                        -- #UNIQUE
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 1, 2, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        -- #UNIQUE
                        test_column
                    FROM test_data
                    """,
                    False,
                ),
                (
                    pd.DataFrame({"test_column": [1, 2, 3, 4, None]}),
                    """
                    SELECT
                        -- #UNIQUE
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3, None, None],
                        }
                    ),
                    """
                    SELECT
                        -- #UNIQUE
                        test_column
                    FROM test_data
                    """,
                    False,
                ),
            ],
            "#UNIQUE_BY": [
                (
                    pd.DataFrame(
                        {
                            "by": ["a", "a", "b", "b", "c"],
                            "col": [1, 1, 2, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        by,
                        -- #UNIQUE_BY(by)
                        col
                    FROM test_data
                    """,
                    False,
                ),
                (
                    pd.DataFrame(
                        {
                            "by": ["a", "a", "b", "b", "c"],
                            "col": [1, 2, 1, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        by,
                        -- #UNIQUE_BY(by)
                        col
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "by": ["a", "a", "a", "b", "c"],
                            "col": [1, None, None, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        by,
                        -- #UNIQUE_BY(by)
                        col
                    FROM test_data
                    """,
                    False,
                ),
            ],
            "#NO_NULLS": [
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3, 4, 5],
                        }
                    ),
                    """
                    SELECT
                        -- #NO_NULLS
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3, 4, None],
                        }
                    ),
                    """
                    SELECT
                        -- #NO_NULLS
                        test_column
                    FROM test_data
                    """,
                    False,
                ),
            ],
            "#SET": [
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        -- #SET{1, 2, 3}
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        -- #SET{1, 2, 3, 4}
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3],
                        }
                    ),
                    """
                    SELECT
                        -- #SET{1, 2}
                        test_column
                    FROM test_data
                    """,
                    False,
                ),
                (
                    pd.DataFrame(
                        {
                            "test_column": [1, 2, 3, None],
                        }
                    ),
                    """
                    SELECT
                        -- #SET{1, 2, 3}
                        test_column
                    FROM test_data
                    """,
                    True,
                ),
            ],
        }.items()
        for i, case in enumerate(cases, start=1)
    ],
)
def test_duckdb_assertions(test_data, query, ok, client):

    view = lea.views.InMemorySQLView(
        key=("tests", "test"),
        query=query,
        client=client,
    )

    client.prepare([view])
    client.materialize_view(view)

    for test in view.yield_assertion_tests():
        conflicts = client.read_sql(test.query)
        assert conflicts.empty == ok
