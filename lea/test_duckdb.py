from __future__ import annotations

import re
from pathlib import Path

import pytest

from lea.conductor import Session
from lea.databases import DuckDBClient, TableStats
from lea.dialects import DuckDBDialect
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
                SELECT * FROM (
                    SELECT UNNEST(
                        [
                            {'id': 1, 'name': 'Alice', 'age': 30},
                            {'id': 2, 'name': 'Bob', 'age': 25},
                            {'id': 3, 'name': 'Charlie', 'age': 35}
                        ], max_depth => 2
                    )
                )
                """,
                sql_dialect=DuckDBDialect(),
            ),
            Script(
                table_ref=TableRef("read", ("core",), "users", "test_project"),
                code="""
                SELECT
                    id,
                    -- #INCREMENTAL
                    name,
                    age
                FROM raw.users
                """,
                sql_dialect=DuckDBDialect(),
            ),
            Script(
                table_ref=TableRef("read", ("analytics",), "n_users", "test_project"),
                code="""
                SELECT COUNT(*)
                FROM core.users
                """,
                sql_dialect=DuckDBDialect(),
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
        unselected_table_refs=set(),
        existing_tables={},
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("raw",), "users", "test_project")]
        ).code,
        """
                        SELECT * FROM (
                    SELECT UNNEST(
                        [
                            {'id': 1, 'name': 'Alice', 'age': 30},
                            {'id': 2, 'name': 'Bob', 'age': 25},
                            {'id': 3, 'name': 'Charlie', 'age': 35}
                        ], max_depth => 2
                    )
                )
        """,
    )
    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM core.users___audit
        """,
    )


def test_incremental_field(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=scripts.keys(),
        unselected_table_refs=set(),
        existing_tables={},
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
            FROM raw.users___audit
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
            FROM core.users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM core.users
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
        unselected_table_refs=set(),
        existing_tables={},
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
        FROM raw.users
        """,
    )


@pytest.mark.duckdb
def test_incremental_field_with_just_incremental_table_selected(scripts):
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("core",), "users", "test_project")},
        unselected_table_refs=set(),
        existing_tables={},
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
            FROM raw.users
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
        unselected_table_refs=set(),
        existing_tables={},
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
            FROM raw.users___audit
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
        unselected_table_refs=set(),
        existing_tables={},
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
            FROM core.users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM core.users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def test_incremental_field_but_no_incremental_table_selected_and_yet_dependency_is_materialized_with_client(
    scripts,
):
    session = Session(
        database_client=DuckDBClient(
            database_path=Path("./test_duckdb"),
            dry_run=False,
            print_mode=False,
        ),
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs={TableRef("read", ("analytics",), "n_users", "test_project")},
        unselected_table_refs=set(),
        existing_tables={},
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
            FROM core.users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM core.users
            WHERE name NOT IN ('Alice')
        )
        """,
    )
