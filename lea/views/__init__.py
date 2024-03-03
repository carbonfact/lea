from __future__ import annotations

import pathlib

import lea

from .base import View
from .json import JSONView
from .python import PythonView
from .sql import InMemorySQLView, SQLView

PATH_SUFFIXES = PythonView.path_suffixes() | SQLView.path_suffixes() | JSONView.path_suffixes()


def open_view_from_path(path, origin, client):
    relative_path = path.relative_to(origin)
    if path.name.split(".", 1)[1] in PythonView.path_suffixes():
        return PythonView(origin=origin, relative_path=relative_path, client=client)
    if path.name.split(".", 1)[1] in SQLView.path_suffixes():
        return SQLView(origin=origin, relative_path=relative_path, client=client)
    if path.name.split(".", 1)[1] in JSONView.path_suffixes():
        return JSONView(origin=origin, relative_path=relative_path, client=client)
    raise ValueError(f"Unsupported view type: {path}")


def open_views(views_dir: pathlib.Path, client: lea.clients.base.Client) -> list[View]:
    return [
        open_view_from_path(path, origin=views_dir, client=client)
        for schema_dir in (d for d in views_dir.iterdir() if d.is_dir())
        for path in schema_dir.rglob("*")
        if not path.is_dir()
        and not path.name.startswith("_")
        and path.name.split(".", 1)[1] in PATH_SUFFIXES
        and path.stat().st_size > 0
    ]


__all__ = [
    "open_views",
    "open_view_from_path",
    "DAGOfViews",
    "View",
    "PythonView",
    "InMemorySQLView",
    "SQLView",
]
