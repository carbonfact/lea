from __future__ import annotations

import collections
import pathlib

import click

import lea


@click.group()
def app():
    pass


@app.command()
@click.option("--select", multiple=True, default=["*"], help="Scripts to materialize.")
@click.option("--unselect", multiple=True, default=[], help="Scripts to skip materializing.")
@click.option("--dataset", default=None, help="Name of the base dataset.")
@click.option("--scripts", default="scripts", help="Directory where the scripts are located.")
@click.option(
    "--incremental", nargs=2, type=str, multiple=True, help="Incremental field name and value."
)
@click.option("--dry", is_flag=True, default=False, help="Whether to run in dry mode.")
@click.option("--print", is_flag=True, default=False, help="Whether to print the SQL code.")
@click.option(
    "--production", is_flag=True, default=False, help="Whether to run the scripts in production."
)
@click.option("--restart", is_flag=True, default=False, help="Whether to restart from scratch.")
@click.option("--env-file", type=click.Path(exists=True), help="Path to the environment file.")
def run(select, unselect, dataset, scripts, incremental, dry, print, production, restart, env_file):
    if select in {"", "Ø"}:
        select = []

    if not pathlib.Path(scripts).is_dir():
        raise click.ClickException(f"Directory {scripts} does not exist")

    # Handle incremental option
    incremental_field_values = collections.defaultdict(set)
    for field, value in incremental:
        incremental_field_values[field].add(value)
    if len(incremental_field_values) > 1:
        raise click.ClickException("Specifying multiple incremental fields is not supported")
    incremental_field_name = next(iter(incremental_field_values), None)
    incremental_field_values = incremental_field_values[incremental_field_name]

    conductor = lea.Conductor(scripts_dir=scripts, dataset_name=dataset, env_file_path=env_file)
    conductor.run(
        select=select,
        unselect=unselect,
        production=production,
        dry_run=dry,
        restart=restart,
        incremental_field_name=incremental_field_name,
        incremental_field_values=incremental_field_values,
        print_mode=print,
    )
