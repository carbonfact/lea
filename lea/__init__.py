from __future__ import annotations

import logging

import click
from rich.logging import RichHandler

from lea import cli, databases
from lea.conductor import Conductor

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            rich_tracebacks=True,
            show_level=False,
            show_path=False,
            markup=True,
            tracebacks_suppress=[click],
        )
    ],
)

log = logging.getLogger("rich")


__all__ = ["cli", "log", "Conductor", "databases"]
