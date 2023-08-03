from __future__ import annotations

import pathlib

import dotenv
import rich.console
import typer

app = typer.Typer()
console = rich.console.Console()


def env_validate_callback(env_path: str | None):
    """

    If a path to .env file is provided, we check that it exists. In any case, we use dotenv
    to load the environment variables.

    """
    if env_path is not None and not pathlib.Path(env_path).exists():
        raise typer.BadParameter(f"File not found: {env_path}")
    dotenv.load_dotenv(env_path, verbose=True)


EnvPath = typer.Option(None, callback=env_validate_callback)


@app.command()
def prepare(production: bool = False, env: str = EnvPath):
    """

    """
    client = _make_client(production)
    client.prepare(console)


@app.command()
def delete_dataset(production: bool = False, env: str = EnvPath):
    """

    HACK: this is just for Carbonfact
    TODO: maybe pass dataset name as an argument? That way it's actually useful

    """

    if production:
        raise ValueError("This is a dangerous operation, so it is not allowed in production.")

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
    env: str = EnvPath
):
    from lea.app.run import run

    # Massage CLI inputs
    only = [tuple(v.split(".")) for v in only] if only else None

    # The client determines where the views will be written
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
def export(views_dir: str, threads: int = 8, env: str = EnvPath):
    """

    HACK: this is too bespoke for Carbonfact

    """
    from lea.app.export import export

    # Massage CLI inputs
    client = _make_client(production=True)

    export(views_dir=views_dir, threads=threads, client=client, console=console)


@app.command()
def test(
    views_dir: str,
    threads: int = 8,
    production: bool = False,
    raise_exceptions: bool = False,
    env: str = EnvPath
):
    from lea.app.test import test

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
def archive(views_dir: str, view: str, env: str = EnvPath):
    from lea.app.archive import archive

    # Massage CLI inputs
    schema, view_name = tuple(view.split("."))

    archive(
        views_dir=views_dir,
        schema=schema,
        view_name=view_name,
    )


@app.command()
def docs(views_dir: str, output_dir: str = "docs", env: str = EnvPath):
    from lea.app.docs import docs

    client = _make_client(production=True)

    docs(views_dir=views_dir, output_dir=output_dir, client=client, console=console)


@app.command()
def diff(origin: str, destination: str, env: str = EnvPath):
    from lea.diff import calculate_diff

    # A client is necessary for getting the top 5 rows of each view
    client = _make_client(production=True)

    diff = calculate_diff(
        origin=origin,
        destination=destination,
        client=client,
    )

    console.print(diff)


def make_app(make_client):

    # This is a hack to make the client available to the commands
    global _make_client
    _make_client = make_client

    return app
