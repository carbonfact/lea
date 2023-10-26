from __future__ import annotations

import pytest
import sqlglot

import lea


@pytest.mark.parametrize(
    "view, expected",
    [
        pytest.param(
            lea.views.GenericSQLView(
                schema=None,
                name=None,
                query=query,
                sqlglot_dialect=sqlglot_dialect,
            ),
            expected,
            id=f"{sqlglot_dialect.name}#{i}",
        )
        for sqlglot_dialect, cases in {
            sqlglot.dialects.Dialects.BIGQUERY: [
                (
                    """
                        SELECT *
                        FROM dataset.schema__table

                        """,
                    {("schema", "table")},
                ),
                (
                    """
                        SELECT *
                        FROM dataset.schema__sub_schema__table

                        """,
                    {("schema", "sub_schema", "table")},
                ),
            ],
            sqlglot.dialects.Dialects.DUCKDB: [
                (
                    """
                        SELECT *
                        FROM schema.table

                        """,
                    {("schema", "table")},
                ),
                (
                    """
                        SELECT *
                        FROM schema.sub_schema__table

                        """,
                    {("schema", "sub_schema", "table")},
                ),
            ],
        }.items()
        for i, (query, expected) in enumerate(cases)
    ],
)
def test_dependency_parsing(view, expected):
    assert view.dependencies == expected
