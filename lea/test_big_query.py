from __future__ import annotations

import re

import pytest
from google.auth.credentials import AnonymousCredentials

from lea.conductor import Session
from lea.databases import BigQueryClient, TableStats
from lea.dialects import BigQueryDialect
from lea.scripts import Script, TableRef

DUMMY_TABLE_STATS = TableStats(n_rows=0, n_bytes=0, updated_at=None)


@pytest.fixture
def scripts() -> dict[TableRef, Script]:
    return {
        script.table_ref: script
        for script in [
            Script(
                table_ref=TableRef("read", ("raw",), "users", "test_project"),
                code="""
                SELECT * FROM UNNEST([
                    STRUCT(1 AS id, 'Alice' AS name, 30 AS age),
                    STRUCT(2 AS id, 'Bob' AS name, 25 AS age),
                    STRUCT(3 AS id, 'Charlie' AS name, 35 AS age)
                ])
                """,
                sql_dialect=BigQueryDialect(),
            ),
            Script(
                table_ref=TableRef("read", ("core",), "users", "test_project"),
                code="""
                SELECT
                    id,
                    -- #INCREMENTAL
                    name,
                    age
                FROM read.raw__users
                """,
                sql_dialect=BigQueryDialect(),
            ),
            Script(
                table_ref=TableRef("read", ("analytics",), "n_users", "test_project"),
                code="""
                SELECT COUNT(*)
                FROM read.core__users
                """,
                sql_dialect=BigQueryDialect(),
            ),
        ]
    }


def assert_queries_are_equal(query1: str, query2: str):
    normalized_query1 = re.sub(r"\s+", " ", query1).strip()
    normalized_query2 = re.sub(r"\s+", " ", query2).strip()
    assert normalized_query1 == normalized_query2


def test_simple_run(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=scripts.keys(),
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("raw",), "users", "test_project")]
        ).code,
        """
        SELECT * FROM UNNEST([
            STRUCT(1 AS id, 'Alice' AS name, 30 AS age),
            STRUCT(2 AS id, 'Bob' AS name, 25 AS age),
            STRUCT(3 AS id, 'Charlie' AS name, 35 AS age)
        ])
        """,
    )
    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM `test_project`.write.core__users___audit
        """,
    )


def test_incremental_field(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=scripts.keys(),
        existing_audit_tables={},
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM `test_project`.write.raw__users___audit
        )
        WHERE name IN ('Alice')
        """,
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*) FROM (
            SELECT *
            FROM `test_project`.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM `test_project`.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def test_incremental_field_but_no_incremental_table_selected(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("analytics",), "n_users", "test_project")},
        existing_audit_tables={},
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT
            id,
            -- #INCREMENTAL
            name,
            age
        FROM `test_project`.write.raw__users
        """,
    )


def test_incremental_field_with_just_incremental_table_selected(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("core",), "users", "test_project")},
        existing_audit_tables={},
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM `test_project`.write.raw__users
        )
        WHERE name IN ('Alice')
        """,
    )


def test_incremental_field_with_just_incremental_table_selected_and_materialized_dependency(
    scripts,
):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("core",), "users", "test_project")},
        existing_audit_tables={
            TableRef("read", ("raw",), "users", "test_project"): DUMMY_TABLE_STATS
        },
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM `test_project`.write.raw__users___audit
        )
        WHERE name IN ('Alice')
        """,
    )


def test_incremental_field_but_no_incremental_table_selected_and_yet_dependency_is_materialized(
    scripts,
):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("analytics",), "n_users", "test_project")},
        existing_audit_tables={
            TableRef("read", ("core",), "users", "test_project"): DUMMY_TABLE_STATS,
        },
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `test_project`.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM `test_project`.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def test_incremental_field_but_no_incremental_table_selected_and_yet_dependency_is_materialized_with_client(
    scripts,
):
    session = Session(
        database_client=BigQueryClient(
            credentials=AnonymousCredentials(),
            location="EU",
            write_project_id="write-project-id",
            compute_project_id="compute-project-id",
        ),
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("analytics",), "n_users", "test_project")},
        existing_audit_tables={
            TableRef("read", ("core",), "users", "test_project"): DUMMY_TABLE_STATS,
        },
        incremental_field_name="name",
        incremental_field_values={"Alice"},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `test_project`.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM `test_project`.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )
