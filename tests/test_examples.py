from __future__ import annotations

import pathlib
import shutil

import duckdb
from typer.testing import CliRunner

from lea.cli import make_app
from lea.clients import make_client

runner = CliRunner()


def test_jaffle_shop(monkeypatch):
    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    views_path = str((here.parent / "examples" / "jaffle_shop" / "views").absolute())

    # Set environment variables
    monkeypatch.setenv("LEA_USERNAME", "max")
    monkeypatch.setenv("LEA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("LEA_DUCKDB_PATH", "tests/jaffle_shop.db")

    # Prepare
    result = runner.invoke(app, ["prepare", views_path])
    assert result.exit_code == 0

    # Run
    result = runner.invoke(app, ["run", views_path, "--fresh"])
    assert result.exit_code == 0

    # Check number of tables created
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
    assert tables.shape[0] == 7

    # Check number of rows in core__customers
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        customers = con.sql("SELECT * FROM core.customers").df()
    assert customers.shape[0] == 100

    # Check number of rows in core__orders
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        orders = con.sql("SELECT * FROM core.orders").df()
    assert orders.shape[0] == 99

    # Run unit tests
    result = runner.invoke(app, ["test", views_path])
    assert result.exit_code == 0
    assert "Found 1 singular tests" in result.stdout
    assert "Found 1 assertion tests" in result.stdout
    assert "SUCCESS" in result.stdout

    # Build docs
    docs_path = here.parent / "examples" / "jaffle_shop" / "docs"
    shutil.rmtree(docs_path, ignore_errors=True)
    result = runner.invoke(
        app,
        ["docs", views_path, "--output-dir", str(docs_path.absolute())],
    )
    assert result.exit_code == 0
    assert docs_path.exists()
    assert (docs_path / "README.md").exists()
    assert (docs_path / "core" / "README.md").exists()
    assert (docs_path / "staging" / "README.md").exists()
    assert (docs_path / "analytics" / "README.md").exists()


def test_jaffle_shop_wap(monkeypatch):
    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    views_path = str((here.parent / "examples" / "jaffle_shop" / "views").absolute())

    # Set environment variables
    monkeypatch.setenv("LEA_USERNAME", "max")
    monkeypatch.setenv("LEA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("LEA_DUCKDB_PATH", "tests/jaffle_shop_wap.db")

    # Prepare
    result = runner.invoke(app, ["prepare", views_path])
    assert result.exit_code == 0

    # Run
    result = runner.invoke(app, ["run", views_path, "--wap"])
    assert result.exit_code == 0

    # Check number of tables created
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
    assert tables.shape[0] == 7

    # Check number of rows in core__customers
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        customers = con.sql("SELECT * FROM core.customers").df()
    assert customers.shape[0] == 100

    # Check number of rows in core__orders
    with duckdb.connect("tests/jaffle_shop_max.db") as con:
        orders = con.sql("SELECT * FROM core.orders").df()
    assert orders.shape[0] == 99


def test_diff(monkeypatch):
    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    prod_views_path = str((here.parent / "examples" / "diff" / "views" / "prod").absolute())
    dev_views_path = str((here.parent / "examples" / "diff" / "views" / "dev").absolute())

    # Set environment variables
    monkeypatch.setenv("LEA_USERNAME", "max")
    monkeypatch.setenv("LEA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("LEA_DUCKDB_PATH", "tests/diff.db")

    # Prepare
    assert runner.invoke(app, ["prepare", prod_views_path, "--production"]).exit_code == 0
    assert runner.invoke(app, ["prepare", dev_views_path]).exit_code == 0

    # Run
    assert runner.invoke(app, ["run", prod_views_path, "--production"]).exit_code == 0
    assert runner.invoke(app, ["run", dev_views_path]).exit_code == 0

    # Check number of tables
    with duckdb.connect("tests/diff.db") as con:
        tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
        assert tables.shape[0] == 5
        assert tables.table_schema.nunique() == 2
    with duckdb.connect("tests/diff_max.db") as con:
        tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
        assert tables.shape[0] == 5
        assert tables.table_schema.nunique() == 3

    # Check number of rows in core__customers
    with duckdb.connect("tests/diff.db") as con:
        assert con.sql("SELECT * FROM core.orders").df().shape[0] == 99
    with duckdb.connect("tests/diff_max.db") as con:
        assert con.sql("SELECT * FROM core.orders").df().shape[0] == 70

    # Check diff
    diff = runner.invoke(app, ["diff", dev_views_path])
    assert diff.exit_code == 0
    assert (
        """+ analytics.kpis
+ 1 rows
+ 1.0B
+ metric
+ value
"""
        in diff.stdout
    )
    assert (
        """- core.customers
- 100 rows
- 100.0B
- customer_id
- customer_lifetime_value
- first_name
- first_order
- last_name
- most_recent_order
- number_of_orders
"""
        in diff.stdout
    )
    assert (
        """  core.orders
- 29 rows
"""
        in diff.stdout
    )


def test_incremental(monkeypatch):
    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    today_views_path = str((here.parent / "examples" / "incremental" / "views_today").absolute())
    tomorrow_views_path = str(
        (here.parent / "examples" / "incremental" / "views_tomorrow").absolute()
    )

    # Set environment variables
    monkeypatch.setenv("LEA_USERNAME", "max")
    monkeypatch.setenv("LEA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("LEA_DUCKDB_PATH", "example/incremental.db")

    # Prepare
    assert runner.invoke(app, ["prepare", today_views_path]).exit_code == 0

    # Run today's data
    assert runner.invoke(app, ["run", today_views_path]).exit_code == 0
    with duckdb.connect("tests/incremental_max.db") as con:
        assert len(con.sql("SELECT * FROM core.events").df()) == 3

    # Run tomorrow's data
    assert runner.invoke(app, ["run", tomorrow_views_path]).exit_code == 0
    with duckdb.connect("tests/incremental_max.db") as con:
        assert len(con.sql("SELECT * FROM core.events").df()) == 4

    # Run tomorrow's data with a full refresh
    assert runner.invoke(app, ["run", tomorrow_views_path, "--no-incremental"]).exit_code == 0
    with duckdb.connect("tests/incremental_max.db") as con:
        assert len(con.sql("SELECT * FROM core.events").df()) == 5


def test_jaffle_shop_materialize_ctes(monkeypatch):
    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    views_path = str((here.parent / "examples" / "jaffle_shop" / "views").absolute())

    monkeypatch.setenv("LEA_USERNAME", "max")
    monkeypatch.setenv("LEA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("LEA_DUCKDB_PATH", "jaffle_shop_ctes.db")

    result = runner.invoke(app, ["prepare", views_path])
    assert (
        result.exit_code == 0
    ), f"Prepare command failed with exit code {result.exit_code}: {result.output}"

    result = runner.invoke(app, ["run", views_path, "--fresh", "--materialize_ctes"])
    print(result.output)

    result = runner.invoke(app, ["run", views_path, "--fresh", "--materialize_ctes"])
    print(result.output)
    assert (
        result.exit_code == 0
    ), f"Run command failed with exit code {result.exit_code}: {result.output}"

    with duckdb.connect("jaffle_shop_ctes_max.db") as con:
        # Get all tables
        tables = con.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('core', 'analytics', 'staging')
        """
        ).fetchall()

        print("\nAll tables:")
        for schema, table in tables:
            print(f"{schema}.{table}")

        # Check for CTE tables
        cte_tables = [f"{schema}.{table}" for schema, table in tables if "__" in table]
        print("\nCTE tables:")
        for table in cte_tables:
            print(table)

        assert len(cte_tables) > 0, "No CTE tables found"

        # Check for specific CTEs
        expected_ctes = ["core.customers__customer_orders", "core.customers__customer_payments"]
        for cte in expected_ctes:
            assert cte in cte_tables, f"{cte} CTE not found"

        # Verify content of CTEs
        for cte_name in expected_ctes:
            try:
                result = con.execute(f"SELECT * FROM {cte_name} LIMIT 5").fetchall()
                print(f"\n{cte_name} data:")
                for row in result:
                    print(row)
                assert result, f"No data found in materialized CTE: {cte_name}"
            except duckdb.CatalogException as e:
                assert False, f"Materialized CTE '{cte_name}' not found or not accessible: {e}"

        try:
            result = con.execute("SELECT * FROM core.customers LIMIT 5").fetchall()
            print("\ncore.customers data:")
            for row in result:
                print(row)
            assert result, "No data found in core.customers view"
        except duckdb.CatalogException as e:
            assert False, f"core.customers view not found or not accessible: {e}"

    print("\nAll CTE materialization tests passed successfully.")