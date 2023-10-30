from __future__ import annotations

import pathlib

import dotenv
import rich.console
import typer

import lea

app = typer.Typer()
console = rich.console.Console()


def env_validate_callback(env_path: str | None):
    """

    If a path to .env file is provided, we check that it exists. In any case, we use dotenv
    to load the environment variables.

    """
    if env_path is not None and not pathlib.Path(env_path).exists():
        raise typer.BadParameter(f"File not found: {env_path}")
    dotenv.load_dotenv(env_path or ".env", verbose=True)


EnvPath = typer.Option(default=None, callback=env_validate_callback)
ViewsDir = typer.Argument(default="views")


@app.command()
def prepare(views_dir: str = ViewsDir, production: bool = False, env: str = EnvPath):
    client = _make_client(production)
    views = lea.views.load_views(views_dir, sqlglot_dialect=client.sqlglot_dialect)
    views = [view for view in views if view.schema not in {"tests", "funcs"}]

    client.prepare(views, console)


@app.command()
def teardown(production: bool = False, env: str = EnvPath):
    """ """

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
    env: str = EnvPath,
):
    from lea.app.run import run

    # The client determines where the views will be written
    client = _make_client(production)

    # Load views
    views = lea.views.load_views(views_dir, sqlglot_dialect=client.sqlglot_dialect)
    views = [view for view in views if view.schema not in {"tests", "funcs"}]

    run(
        client=client,
        views=views,
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
    env: str = EnvPath,
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
def docs(
    views_dir: str = ViewsDir,
    output_dir: str = "docs",
    production: bool = False,
    env: str = EnvPath,
):
    from lea.app.docs import docs

    client = _make_client(production=production)

    docs(views_dir=views_dir, output_dir=output_dir, client=client, console=console)


@app.command()
def diff(env: str = EnvPath):
    from lea.app.diff import calculate_diff

    diff = calculate_diff(
        origin_client=_make_client(production=False),
        target_client=_make_client(production=True),
    )

    console.print(diff)


def make_app(make_client):
    # This is a hack to make the client available to the commands
    global _make_client
    _make_client = make_client

    return app
