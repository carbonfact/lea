from __future__ import annotations

import re

import pytest
from google.auth.credentials import AnonymousCredentials

from lea.conductor import Session
from lea.databases import ON_DEMAND_RESERVATION, BigBluePickAPI, BigQueryClient, TableStats
from lea.dialects import BigQueryDialect
from lea.scripts import Script, SQLScript, TableRef

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
            Script(
                table_ref=TableRef("read", ("analytics",), "n_users_with_unnest", "test_project"),
                code="""
                SELECT COUNT(*)
                FROM read.core__users, UNNEST([1, 2, 3]) AS n
                """,
                sql_dialect=BigQueryDialect(),
            ),
        ]
    }


def assert_queries_are_equal(query1: str, query2: str):
    assert normalize_query(query1) == normalize_query(query2)


def normalize_query(query: str) -> str:
    normalized_query = re.sub(r"\s+", " ", query).strip()
    normalized_query = re.sub(r"/\*.*?\*/", "", normalized_query)
    normalized_query = normalized_query.replace("  ", " ")
    normalized_query = normalized_query.strip()
    return normalized_query


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
        FROM test_project.write.core__users___audit
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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM test_project.write.raw__users___audit
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
            FROM test_project.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM test_project.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def test_incremental_field_with_comma(scripts):
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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM test_project.write.raw__users___audit
        )
        WHERE name IN ('Alice')
        """,
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users_with_unnest", "test_project")]
        ).code,
        """
        SELECT COUNT(*) FROM (
            SELECT *
            FROM test_project.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM test_project.write.core__users
            WHERE name NOT IN ('Alice')
        ) , UNNEST([1, 2, 3]) AS n
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
        incremental_field_values=["Alice"],
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
        FROM test_project.write.raw__users
        """,
    )


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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM test_project.write.raw__users
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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("core",), "users", "test_project")]
        ).code,
        """
        SELECT *
        FROM (
            SELECT id, name, age
            FROM test_project.write.raw__users___audit
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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM test_project.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM test_project.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def test_incremental_field_but_no_incremental_table_selected_and_yet_dependency_is_materialized_with_client(
    scripts,
):
    session = Session(
        database_client=BigQueryClient(
            credentials=AnonymousCredentials(),  # ty: ignore[invalid-argument-type]
            location="EU",
            write_project_id="write-project-id",
            compute_project_id="compute-project-id",
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
        incremental_field_values=["Alice"],
    )

    assert_queries_are_equal(
        session.add_context_to_script(
            scripts[TableRef("read", ("analytics",), "n_users", "test_project")]
        ).code,
        """
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM test_project.write.core__users___audit
            WHERE name IN ('Alice')

            UNION ALL

            SELECT *
            FROM test_project.write.core__users
            WHERE name NOT IN ('Alice')
        )
        """,
    )


def _make_client(**kwargs) -> BigQueryClient:
    return BigQueryClient(
        credentials=AnonymousCredentials(),  # ty: ignore[invalid-argument-type]
        location="EU",
        write_project_id="write-project-id",
        compute_project_id="compute-project-id",
        **kwargs,
    )


def _sql_script(table_ref: TableRef) -> SQLScript:
    return SQLScript(
        table_ref=table_ref, code="SELECT 1", sql_dialect=BigQueryDialect(), fields=[]
    )


def test_determine_reservation_no_overrides():
    """No Pick API, no script-specific map → the query inherits the project's assignment."""
    client = _make_client()
    assert (
        client.determine_reservation_for_script(
            _sql_script(TableRef("read", ("core",), "users", "test_project"))
        )
        is None
    )


def test_determine_reservation_script_specific():
    """A mapping entry for this table_ref is used verbatim."""
    table_ref = TableRef("read", ("core",), "users", "test_project")
    reservation = "projects/P/locations/EU/reservations/R"
    client = _make_client(script_specific_reservations={table_ref: reservation})

    assert client.determine_reservation_for_script(_sql_script(table_ref)) == reservation
    # Unrelated tables still inherit the project assignment.
    other = TableRef("read", ("core",), "accounts", "test_project")
    assert client.determine_reservation_for_script(_sql_script(other)) is None


def test_script_specific_wins_over_pick_api(monkeypatch):
    """Explicit per-script overrides trump whatever Pick API suggests."""
    table_ref = TableRef("read", ("core",), "users", "test_project")
    static_reservation = "projects/P/locations/EU/reservations/static"
    client = _make_client(script_specific_reservations={table_ref: static_reservation})
    client.big_blue_pick_api = BigBluePickAPI(
        api_url="https://pick.example",
        api_key="k",
        reservation="projects/P/locations/EU/reservations/pick",
    )
    # Pick API would say ON-DEMAND, but the static mapping must win.
    monkeypatch.setattr(
        BigBluePickAPI, "call_pick_api", lambda self, path, body: {"pick": "ON-DEMAND"}
    )

    assert client.determine_reservation_for_script(_sql_script(table_ref)) == static_reservation


def test_pick_api_used_when_no_script_specific(monkeypatch):
    """Pick API fills in for tables not listed in the static mapping."""
    table_ref = TableRef("read", ("core",), "users", "test_project")
    client = _make_client()
    client.big_blue_pick_api = BigBluePickAPI(
        api_url="https://pick.example",
        api_key="k",
        reservation="projects/P/locations/EU/reservations/pick",
    )
    monkeypatch.setattr(
        BigBluePickAPI, "call_pick_api", lambda self, path, body: {"pick": "ON-DEMAND"}
    )

    assert (
        client.determine_reservation_for_script(_sql_script(table_ref))
        == ON_DEMAND_RESERVATION
    )
