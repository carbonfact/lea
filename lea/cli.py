import collections

import click
import dotenv

from lea.conductor import Conductor


@click.group()
def cli():
    dotenv.load_dotenv(".env", verbose=True)


@cli.command()
@click.option('--select', '-m', multiple=True)
@click.option('--dataset', default=None, help='Name of the base dataset.')
@click.option('--scripts', default='scripts', help='Directory where the scripts are located.')
@click.option('--incremental', nargs=2, type=str, multiple=True, help='Incremental field name and value.')
@click.option('--dry', is_flag=True, default=False, help='Whether to run in dry mode.')
@click.option('--keep-going', is_flag=True, default=False, help='Whether to keep going after an error.')
@click.option('--fresh', is_flag=True, default=False, help='Whether to start from scratch.')
@click.option('--defer', is_flag=True, default=False, help='Whether to use production data when unselected.')
def run(select, dataset, scripts, incremental, dry, keep_going, fresh, defer):

    # Handle incremental option
    incremental_field_values = collections.defaultdict(set)
    for field, value in incremental:
        incremental_field_values[field].add(value)
    if len(incremental_field_values) > 1:
        raise ValueError("Specifying multiple incremental fields is not supported.")
    incremental_field_name = next(iter(incremental_field_values), None)
    incremental_field_values = incremental_field_values[incremental_field_name]

    conductor = Conductor(scripts_dir=scripts, dataset_name=dataset, )
    conductor.run(
        *select,
        dry_run=dry,
        keep_going=keep_going,
        fresh=fresh,
        defer_mode=defer,
        incremental_field_name=incremental_field_name,
        incremental_field_values=incremental_field_values,
    )


if __name__ == '__main__':
    cli()

    # python lea/lea/cli.py run --select core.accounts+ --dataset kaya --scripts views
    # python lea/lea/cli.py run --select core.accounts --dataset kaya --scripts views --fresh --incremental account_slug demo-account --incremental account_slug adore-me
    # python lea/lea/cli.py run --select measure.metrics_cube --dataset kaya --scripts views --fresh --defer
