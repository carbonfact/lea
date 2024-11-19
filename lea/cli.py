from __future__ import annotations

import collections
import pathlib

import click

import lea


@click.group()
def app():
    ...


@app.command()
@click.option("--select", "-m", multiple=True, default=["*"], help="Scripts to materialize.")
@click.option("--dataset", default=None, help="Name of the base dataset.")
@click.option("--scripts", default="scripts", help="Directory where the scripts are located.")
@click.option(
    "--incremental", nargs=2, type=str, multiple=True, help="Incremental field name and value."
)
@click.option("--stateful", is_flag=True, default=False, help="Whether to start from scratch.")
@click.option("--dry", is_flag=True, default=False, help="Whether to run in dry mode.")
@click.option(
    "--keep-going", is_flag=True, default=False, help="Whether to keep going after an error."
)
@click.option("--print", is_flag=True, default=False, help="Whether to print the SQL code.")
@click.option("--production", is_flag=True, default=False, help="Whether to run the scripts in production.")
def run(select, dataset, scripts, incremental, stateful, dry, keep_going, print, production):
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

    conductor = lea.Conductor(scripts_dir=scripts, dataset_name=dataset)
    conductor.run(
        *select,
        production=production,
        dry_run=dry,
        keep_going=keep_going,
        stateful=stateful,
        incremental_field_name=incremental_field_name,
        incremental_field_values=incremental_field_values,
        print_mode=print,
    )
