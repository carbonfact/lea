from __future__ import annotations

import pathlib

import dotenv
import rich.console
import typer

app = typer.Typer()
console = rich.console.Console()


def env_validate_callback(env_path: str | None = ".env"):
    """

    If a path to .env file is provided, we check that it exists. In any case, we use dotenv
    to load the environment variables.

    """
    if env_path is not None and not pathlib.Path(env_path).exists():
        raise typer.BadParameter(f"File not found: {env_path}")
    dotenv.load_dotenv(env_path, verbose=True)


EnvPath = typer.Option(default=".env", callback=env_validate_callback)
ViewsDir = typer.Option(default="views")


@app.command()
def prepare(production: bool = False, env: str = EnvPath):
    """

    """
    client = _make_client(production)
    client.prepare(console)


@app.command()
def teardown(production: bool = False, env: str = EnvPath):
    """

    """

    if production:
        raise ValueError("This is a dangerous operation, so it is not allowed in production.")

    client = _make_client(production)

    # Create the dataset
    client.teardown()


@app.command()
def run(
    views_dir: str = ViewsDir,
    only: list[str] = typer.Option(default=None),
    dry: bool = False,
    print: bool = False,
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
        print_to_cli=print,
        fresh=fresh,
        threads=threads,
        show=show,
        raise_exceptions=raise_exceptions,
        console=console,
    )


@app.command()
def test(
    views_dir: str = ViewsDir,
    only: list[str] = typer.Option(None),
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
        only=only,
        threads=threads,
        raise_exceptions=raise_exceptions,
        console=console,
    )


@app.command()
def docs(views_dir: str = ViewsDir, output_dir: str = "docs", env: str = EnvPath):
    from lea.app.docs import docs

    client = _make_client(production=True)

    docs(views_dir=views_dir, output_dir=output_dir, client=client, console=console)



@app.command()
def diff(origin: str, destination: str, env: str = EnvPath):
    from lea.app.diff import calculate_diff

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
