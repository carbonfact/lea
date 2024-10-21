from __future__ import annotations

import pytest

import lea


@pytest.mark.parametrize(
    "client, query, expected",
    [
        pytest.param(
            client,
            query,
            expected,
            id=f"{client.sqlglot_dialect}#{i}",
        )
        for client, cases in [
            (
                lea.clients.BigQuery(
                    credentials=None,
                    location=None,
                    compute_project_id=None,
                    write_project_id=None,
                    dataset_name="dataset",
                    username="max",
                    wap_mode=False,
                ),
                [
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
            ),
            (
                lea.clients.DuckDB(":memory:", username=None),
                [
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
            ),
        ]
        for i, (query, expected) in enumerate(cases)
    ],
)
def test_dependency_parsing(client, query, expected):
    view = lea.views.InMemorySQLView(
        key=("tests", "test"),
        query=query,
        client=client,
    )

    assert view.dependent_view_keys == expected
