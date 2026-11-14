from __future__ import annotations

import re

import pytest
from google.auth.credentials import AnonymousCredentials

from lea.conductor import Session
from lea.dag import DAGOfScripts
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


@pytest.mark.parametrize(
    "foreign_ref",
    [
        "test_project.other_dataset.core__users",
        "other_project.read.core__users",
    ],
)
def test_foreign_dependency_is_left_alone(scripts, foreign_ref):
    """A dependency written with its project, in another dataset or project, is not rewritten,
    even if a local script has the same name."""
    compare = Script(
        table_ref=TableRef("read", ("analytics",), "compare_users", "test_project"),
        code=f"""
        SELECT core__users.id, foreign_users.id
        FROM read.core__users
        JOIN test_project.read.core__users USING (id)
        FULL OUTER JOIN {foreign_ref} AS foreign_users USING (id)
        """,
        sql_dialect=BigQueryDialect(),
    )
    scripts = {**scripts, compare.table_ref: compare}
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=set(scripts),
        unselected_table_refs=set(),
        existing_tables={},
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(compare).code,
        f"""
        SELECT core__users___audit.id, foreign_users.id
        FROM test_project.write.core__users___audit
        JOIN test_project.write.core__users___audit USING (id)
        FULL OUTER JOIN {foreign_ref} AS foreign_users USING (id)
        """,
    )


def test_dependency_without_project_is_local_whatever_its_dataset(scripts):
    """The code may name a dataset other than the one being built, for instance when the same
    scripts are built into a stable dataset. Without a project, the dependency is still local."""
    compare = Script(
        table_ref=TableRef("read", ("analytics",), "compare_users", "test_project"),
        code="SELECT id FROM other_dataset.core__users",
        sql_dialect=BigQueryDialect(),
    )
    scripts = {**scripts, compare.table_ref: compare}
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=set(scripts),
        unselected_table_refs=set(),
        existing_tables={},
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(compare).code,
        "SELECT id FROM test_project.write.core__users___audit",
    )


def test_same_table_with_and_without_project(scripts):
    """Each way of writing the table is handled on its own: without a project it is local, with
    its project and another dataset it is external."""
    compare = Script(
        table_ref=TableRef("read", ("analytics",), "compare_users", "test_project"),
        code="""
        SELECT *
        FROM other_dataset.core__users
        JOIN test_project.other_dataset.core__users USING (id)
        """,
        sql_dialect=BigQueryDialect(),
    )
    scripts = {**scripts, compare.table_ref: compare}
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=set(scripts),
        unselected_table_refs=set(),
        existing_tables={},
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(compare).code,
        """
        SELECT *
        FROM test_project.write.core__users___audit
        JOIN test_project.other_dataset.core__users USING (id)
        """,
    )


def test_foreign_dataset_dependency_is_not_a_dag_edge(tmp_path):
    scripts_dir = tmp_path / "scripts"
    (scripts_dir / "core").mkdir(parents=True)
    (scripts_dir / "core" / "users.sql").write_text("SELECT 1 AS id")
    (scripts_dir / "analytics").mkdir()
    (scripts_dir / "analytics" / "compare_users.sql").write_text(
        """
        SELECT *
        FROM other_dataset.core__users
        JOIN test_project.other_dataset.core__users USING (id)
        JOIN test_project.read.core__users USING (id)
        """
    )

    dag = DAGOfScripts.from_directory(
        scripts_dir=scripts_dir,
        sql_dialect=BigQueryDialect(),
        dataset_name="read",
        project_name="test_project",
    )

    assert dag.dependency_graph[
        TableRef("read", ("analytics",), "compare_users", "test_project")
    ] == {
        TableRef("read", ("core",), "users", "test_project"),
        TableRef("other_dataset", ("core",), "users", "test_project", project_is_explicit=True),
    }


def test_quoted_references_with_hyphenated_project():
    """Each way of quoting a reference in BigQuery, with a project that needs backticks."""
    users = Script(
        table_ref=TableRef("read", ("core",), "users", "my-project"),
        code="SELECT 1 AS id",
        sql_dialect=BigQueryDialect(),
    )
    compare = Script(
        table_ref=TableRef("read", ("analytics",), "compare_users", "my-project"),
        code="""
        SELECT *
        FROM `my-project`.read.core__users
        JOIN my-project.read.core__users USING (id)
        JOIN `read`.core__users USING (id)
        JOIN `my-project.read.core__users` USING (id)
        JOIN `my-project`.read_stable.core__users USING (id)
        JOIN `my-project.read_stable.core__users` USING (id)
        """,
        sql_dialect=BigQueryDialect(),
    )
    scripts = {users.table_ref: users, compare.table_ref: compare}
    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=set(scripts),
        unselected_table_refs=set(),
        existing_tables={},
        existing_audit_tables={},
    )

    assert_queries_are_equal(
        session.add_context_to_script(compare).code,
        """
        SELECT *
        FROM my-project.write.core__users___audit
        JOIN my-project.write.core__users___audit USING (id)
        JOIN my-project.write.core__users___audit USING (id)
        JOIN `my-project.write.core__users___audit` USING (id)
        JOIN `my-project`.read_stable.core__users USING (id)
        JOIN `my-project.read_stable.core__users` USING (id)
        """,
    )


def test_warehouse_table_ref_matches_script_table_ref():
    """Table refs listed from the warehouse name their project, but are not written in code."""
    assert BigQueryDialect.parse_table_ref("test_project.read.core__users") == TableRef(
        "read", ("core",), "users", "test_project"
    )
