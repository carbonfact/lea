from __future__ import annotations

import pathlib

import dotenv
import typer

import lea

app = typer.Typer()


def validate_env_path(env_path: str | None):
    """

    If a path to .env file is provided, we check that it exists. In any case, we use dotenv
    to load the environment variables.

    """
    if env_path is not None and not pathlib.Path(env_path).exists():
        raise typer.BadParameter(f"File not found: {env_path}")
    dotenv.load_dotenv(env_path or ".env", verbose=True)


def validate_views_dir(views_dir: str):
    if not pathlib.Path(views_dir).exists():
        raise typer.BadParameter(f"Directory not found: {views_dir}")
    return views_dir


EnvPath = typer.Option(default=None, callback=validate_env_path)
ViewsDir = typer.Argument(default="views", callback=validate_views_dir)


@app.command()
def prepare(views_dir: str = ViewsDir, production: bool = False, env: str = EnvPath):
    client = _make_client(production, wap_mode=False)
    runner = lea.Runner(views_dir=views_dir, client=client, verbose=True)
    runner.prepare()


@app.command()
def teardown(production: bool = False, env: str = EnvPath):
    if production:
        raise ValueError(
            """
        This is a dangerous operation, so it is not allowed in production. If you really want to
        do this, then do so manually.
        """
        )

    client = _make_client(production)
    client.teardown()


@app.command()
def run(
    views_dir: str = ViewsDir,
    select: list[str] = typer.Option(default=None),
    freeze_unselected: bool = False,
    dry: bool = False,
    print: bool = False,
    silent: bool = False,
    fresh: bool = False,
    production: bool = False,
    threads: int = 8,
    show: int = 20,
    fail_fast: bool = False,
    wap: bool = False,
    incremental: bool = True,
    materialize_ctes: bool = typer.Option(False, "--materialize_ctes", help="Materialize CTEs as separate tables"),
    env: str = EnvPath,
):
    client = _make_client(production, wap_mode=wap)
    runner = lea.Runner(views_dir=views_dir, client=client, verbose=not silent and not print)
    runner.run(
        select=select,
        freeze_unselected=freeze_unselected,
        dry=dry,
        print_views=print,
        fresh=fresh,
        threads=threads,
        show=show,
        fail_fast=fail_fast,
        incremental=incremental,
        materialize_ctes=materialize_ctes,
    )


@app.command()
def test(
    views_dir: str = ViewsDir,
    select_views: list[str] = typer.Option(None),
    freeze_unselected: bool = False,
    threads: int = 8,
    production: bool = False,
    fail_fast: bool = False,
    env: str = EnvPath,
):
    client = _make_client(production)
    runner = lea.Runner(views_dir=views_dir, client=client, verbose=True)
    runner.test(
        select_views=select_views,
        freeze_unselected=freeze_unselected,
        threads=threads,
        fail_fast=fail_fast,
    )


@app.command()
def docs(
    views_dir: str = ViewsDir,
    output_dir: str = "docs",
    production: bool = False,
    env: str = EnvPath,
):
    client = _make_client(production)
    runner = lea.Runner(views_dir=views_dir, client=client)
    runner.make_docs(output_dir=output_dir)


@app.command()
def diff(
    views_dir: str = ViewsDir, select: list[str] = typer.Option(default=None), env: str = EnvPath
):
    client = _make_client(production=False)
    runner = lea.Runner(views_dir=views_dir, client=client)
    diff = runner.calculate_diff(select=select, target_client=_make_client(production=True))

    print(diff)


def make_app(make_client):
    # This is a hack to make the client available to the commands
    global _make_client
    _make_client = make_client

    return app
