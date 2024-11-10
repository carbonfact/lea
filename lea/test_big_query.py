import re
import pytest

from lea.conductor import Session
from lea.scripts import TableRef, Script
from lea.dialects import BigQueryDialect


@pytest.fixture
def scripts() -> dict[TableRef, Script]:
    return {
        script.table_ref: script
        for script in [
            Script(
                table_ref=TableRef("read", ("raw",), "users"),
                code="""
                SELECT * FROM UNNEST([
                    STRUCT(1 AS id, 'Alice' AS name, 30 AS age),
                    STRUCT(2 AS id, 'Bob' AS name, 25 AS age),
                    STRUCT(3 AS id, 'Charlie' AS name, 35 AS age)
                ])
                """,
                sql_dialect=BigQueryDialect
            ),
            Script(
                table_ref=TableRef("read", ("core",), "users"),
                code="""
                SELECT
                    id,
                    -- #INCREMENTAL
                    name,
                    age
                FROM read.raw__users
                """,
                sql_dialect=BigQueryDialect
            ),
            Script(
                table_ref=TableRef("read", ("analytics",), "n_users"),
                code="""
                SELECT COUNT(*)
                FROM read.core__users
                """,
                sql_dialect=BigQueryDialect
            )
        ]
    }


def assert_queries_are_equal(query1: str, query2: str):
    normalized_query1 = re.sub(r'\s+', ' ', query1).strip()
    normalized_query2 = re.sub(r'\s+', ' ', query2).strip()
    assert normalized_query1 == normalized_query2


def test_simple_run(scripts):

    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=scripts.keys()
    )

    assert_queries_are_equal(
        session.add_context(scripts[TableRef("read", ("raw",), "users")]).code,
        """
        SELECT * FROM UNNEST([
            STRUCT(1 AS id, 'Alice' AS name, 30 AS age),
            STRUCT(2 AS id, 'Bob' AS name, 25 AS age),
            STRUCT(3 AS id, 'Charlie' AS name, 35 AS age)
        ])
        """
    )
    assert_queries_are_equal(
        session.add_context(scripts[TableRef("read", ("analytics",), "n_users")]).code,
        """
        SELECT COUNT(*)
        FROM write.core__users___audit
        """
    )


def test_incremental_field(scripts):

    session = Session(
        database_client=None,
        base_dataset="read",
        write_dataset="write",
        scripts=scripts,
        selected_table_refs=scripts.keys(),
        incremental_field_name="name",
        incremental_field_values={"Alice"}
    )

    assert_queries_are_equal(
        session.add_context(scripts[TableRef("read", ("core",), "users")]).code,
        """

        """
    )
