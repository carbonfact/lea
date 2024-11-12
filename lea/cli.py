import collections
import pathlib

import click
import dotenv

import lea


@click.group()
def app():
    dotenv.load_dotenv(".env", verbose=True)


def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            click.secho(f"An error occurred: {e}", fg="red")
            raise click.Abort()
    return wrapper


@app.command()
@click.option('--select', '-m', multiple=True)
@click.option('--dataset', default=None, help='Name of the base dataset.')
@click.option('--scripts', default='scripts', help='Directory where the scripts are located.')
@click.option('--incremental', nargs=2, type=str, multiple=True, help='Incremental field name and value.')
@click.option('--dry', is_flag=True, default=False, help='Whether to run in dry mode.')
@click.option('--keep-going', is_flag=True, default=False, help='Whether to keep going after an error.')
@click.option('--fresh', is_flag=True, default=False, help='Whether to start from scratch.')
def run(select, dataset, scripts, incremental, dry, keep_going, fresh):

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
        dry_run=dry,
        keep_going=keep_going,
        fresh=fresh,
        incremental_field_name=incremental_field_name,
        incremental_field_values=incremental_field_values,
    )
