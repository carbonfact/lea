from __future__ import annotations

import collections
import os
import subprocess

import click
import dotenv

import lea


@click.group()
def app():
    """A minimalistic SQL orchestrator."""


@app.command()
@click.option("--select", multiple=True, default=["*"], help="Scripts to materialize.")
@click.option("--unselect", multiple=True, default=[], help="Scripts to skip.")
@click.option("--dataset", default=None, help="Base dataset name.")
@click.option(
    "--scripts",
    default="scripts",
    type=click.Path(exists=True, file_okay=False),
    help="Directory where scripts are located.",
)
@click.option(
    "--incremental", nargs=2, type=str, multiple=True, help="Incremental field name and value."
)
@click.option("--dry-run", is_flag=True, default=False, help="Run without materializing.")
@click.option("--print", "print_mode", is_flag=True, default=False, help="Print the SQL code.")
@click.option(
    "--production", is_flag=True, default=False, help="Target the production environment."
)
@click.option("--restart", is_flag=True, default=False, help="Restart from scratch.")
@click.option("--quack", is_flag=True, default=False, help="Run locally with DuckDB/DuckLake.")
@click.option(
    "--quack-push",
    is_flag=True,
    default=False,
    help="Push DuckLake tables to the native warehouse.",
)
@click.option("--env-file", type=click.Path(exists=True), help="Path to an environment file.")
def run(
    select,
    unselect,
    dataset,
    scripts,
    incremental,
    dry_run,
    print_mode,
    production,
    restart,
    quack,
    quack_push,
    env_file,
):
    """Run SQL scripts in dependency order."""
    if select in {"", "Ø"}:
        select = []

    # Handle incremental option
    incremental_field_values = collections.defaultdict(set)
    for field, value in incremental:
        incremental_field_values[field].add(value)
    if len(incremental_field_values) > 1:
        raise click.ClickException("Specifying multiple incremental fields is not supported")
    incremental_field_name = next(iter(incremental_field_values), None)
    incremental_field_values = incremental_field_values[incremental_field_name]

    if quack_push and not quack:
        quack = True

    conductor = lea.Conductor(scripts_dir=scripts, dataset_name=dataset, env_file_path=env_file)
    conductor.run(
        select=select,
        unselect=unselect,
        production=production,
        dry_run=dry_run,
        restart=restart,
        incremental_field_name=incremental_field_name,
        incremental_field_values=incremental_field_values,
        print_mode=print_mode,
        quack=quack,
        quack_push=quack_push,
    )


@app.command("quack-ui")
@click.option("--env-file", type=click.Path(exists=True), help="Path to the environment file.")
def quack_ui(env_file):
    """Open the DuckDB UI with the DuckLake catalog attached."""
    dotenv.load_dotenv(env_file or ".env", verbose=True)

    catalog = os.environ.get("LEA_QUACK_DUCKLAKE_CATALOG_DATABASE")
    data_path = os.environ.get("LEA_QUACK_DUCKLAKE_DATA_PATH")
    if not catalog or not data_path:
        raise click.ClickException(
            "LEA_QUACK_DUCKLAKE_CATALOG_DATABASE and LEA_QUACK_DUCKLAKE_DATA_PATH must be set"
        )

    setup_sql = (
        "INSTALL ducklake; LOAD ducklake; "
        "INSTALL ui FROM core_nightly; LOAD ui; "
        f"ATTACH 'ducklake:{catalog}' AS quack_ducklake (DATA_PATH '{data_path}', AUTOMATIC_MIGRATION TRUE); "
        "USE quack_ducklake;"
    )
    s3_endpoint = os.environ.get("LEA_QUACK_DUCKLAKE_S3_ENDPOINT")
    if s3_endpoint:
        setup_sql = f"SET s3_endpoint='{s3_endpoint}'; " + setup_sql

    subprocess.run(["duckdb", "-cmd", setup_sql, "-ui"])
