from __future__ import annotations

import getpass
import json
import os

import dotenv
import rich.console
import rich.live
import rich.table
import typer

import lea

dotenv.load_dotenv(verbose=True)
app = typer.Typer()
console = rich.console.Console()


def _get_username():
    # Default to who
    return str(os.environ.get("LEA_USERNAME", getpass.getuser()))


def _make_client(production: bool):

    warehouse = os.environ["LEA_WAREHOUSE"]
    username = None if production else str(os.environ.get("LEA_USERNAME", getpass.getuser()))

    if warehouse == "bigquery":
        # Do imports here to avoid loading them all the time
        from google.oauth2 import service_account
        from lea.clients.bigquery import BigQuery

        return BigQuery(
            credentials=service_account.Credentials.from_service_account_info(
                json.loads(os.environ["LEA_BQ_SERVICE_ACCOUNT"])
            ),
            location=os.environ["LEA_BQ_LOCATION"],
            project_id=os.environ["LEA_BQ_PROJECT_ID"],
            dataset_name=os.environ["LEA_SCHEMA"],
            username=username,
        )
    else:
        raise ValueError(f"Unsupported warehouse: {warehouse}")


@app.command()
def prepare(production: bool = False):
    """

    """
    client = _make_client(production)
    client.prepare()


@app.command()
def delete_dataset(production: bool = False):
    """

    HACK: this is just for Carbonfact
    TODO: maybe pass dataset name as an argument? That way it's actually useful

    """

    if production:
        raise ValueError("This is a dangerous operation, so it is not allowed in production.")

    # The client determines where the views will be written
    # TODO: move this to a config file
    client = _make_client(production)

    # Create the dataset
    client.delete_dataset()


@app.command()
def run(
    views_dir: str,
    only: list[str] = typer.Option(None),
    dry: bool = False,
    fresh: bool = False,
    production: bool = False,
    threads: int = 8,
    show: int = 20,
    raise_exceptions: bool = False,
):
    from lea.commands.run import run

    # Massage CLI inputs
    only = [tuple(v.split(".")) for v in only] if only else None

    # The client determines where the views will be written
    # TODO: move this to a config file
    client = _make_client(production)

    run(
        client=client,
        views_dir=views_dir,
        only=only,
        dry=dry,
        fresh=fresh,
        threads=threads,
        show=show,
        raise_exceptions=raise_exceptions,
        console=console,
    )


@app.command()
def export(views_dir: str, threads: int = 8):
    """

    HACK: this is too bespoke for Carbonfact

    """
    from lea.commands.export import export

    # Massage CLI inputs
    client = _make_client(production=True)

    export(views_dir=views_dir, threads=threads, client=client, console=console)


@app.command()
def test(
    views_dir: str,
    threads: int = 8,
    production: bool = False,
    raise_exceptions: bool = False,
):
    from lea.commands.test import test

    # A client is necessary for running tests, because each test is a query
    client = _make_client(production)

    test(
        client=client,
        views_dir=views_dir,
        threads=threads,
        raise_exceptions=raise_exceptions,
        console=console,
    )


@app.command()
def archive(views_dir: str, view: str):
    from lea.commands.archive import archive

    # Massage CLI inputs
    schema, view_name = tuple(view.split("."))

    archive(
        views_dir=views_dir,
        schema=schema,
        view_name=view_name,
    )


@app.command()
def docs(views_dir: str, output_dir: str = "docs"):
    from lea.commands.docs import docs

    client = _make_client(production=True)

    docs(views_dir=views_dir, output_dir=output_dir, client=client, console=console)


@app.command()
def diff(origin: str, destination: str):
    from lea.diff import calculate_diff

    # A client is necessary for getting the top 5 rows of each view
    client = _make_client(production=True)

    diff = calculate_diff(
        origin=origin,
        destination=destination,
        client=client,
    )

    console.print(diff)
