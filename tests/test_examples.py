from __future__ import annotations

import pathlib

import duckdb
from typer.testing import CliRunner

from lea.app import make_app
from lea.clients import make_client

runner = CliRunner()

def test_jaffle_shop():

    app = make_app(make_client=make_client)
    here = pathlib.Path(__file__).parent
    env_path = str((here.parent / "examples" / "jaffle_shop" / ".env").absolute())
    views_path = str((here.parent / "examples" / "jaffle_shop" / "views").absolute())

    # Write .env file
    with open(env_path, "w") as f:
        f.write(
            "LEA_SCHEMA=jaffle_shop\n"
            "LEA_USERNAME=max\n"
            "LEA_WAREHOUSE=duckdb\n"
            "LEA_DUCKDB_PATH=duckdb.db\n"
        )

    # Prepare
    result = runner.invoke(app, ["prepare", "--env", env_path])
    assert result.exit_code == 0

    # RUn
    result = runner.invoke(app, ["run", views_path, "--env", env_path])
    assert result.exit_code == 0

    # Check number of tables created
    con = duckdb.connect("duckdb.db")
    tables = con.sql("SELECT table_schema, table_name FROM information_schema.tables").df()
    assert tables.shape[0] == 5

    # Check number of rows in core__customers
    customers = con.sql("SELECT * FROM jaffle_shop_max.core__customers").df()
    assert customers.shape[0] == 100

    # Check number of rows in core__orders
    orders = con.sql("SELECT * FROM jaffle_shop_max.core__orders").df()
    assert orders.shape[0] == 99
