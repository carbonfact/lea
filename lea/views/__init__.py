from __future__ import annotations

import pathlib

import sqlglot

from .base import View
from .python import PythonView
from .json import JSONView
from .sql import GenericSQLView, SQLView

PATH_SUFFIXES = PythonView.path_suffixes() | SQLView.path_suffixes() | JSONView.path_suffixes()


def open_view_from_path(path, origin, sqlglot_dialect):
    relative_path = path.relative_to(origin)
    if path.name.split(".", 1)[1] in PythonView.path_suffixes():
        return PythonView(origin, relative_path)
    if path.name.split(".", 1)[1] in SQLView.path_suffixes():
        return SQLView(origin, relative_path, sqlglot_dialect=sqlglot_dialect)
    if path.name.split(".", 1)[1] in JSONView.path_suffixes():
        return JSONView(origin, relative_path)
    raise ValueError(f"Unsupported view type: {path}")


def open_views(
    views_dir: pathlib.Path | str, sqlglot_dialect: sqlglot.dialects.Dialect | str
) -> list[View]:
    # Massage the inputs
    if isinstance(views_dir, str):
        views_dir = pathlib.Path(views_dir)
    if isinstance(sqlglot_dialect, str):
        sqlglot_dialect = sqlglot.dialects.Dialects(sqlglot_dialect)

    return [
        open_view_from_path(path, origin=views_dir, sqlglot_dialect=sqlglot_dialect)
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
    "SQLView",
    "GenericSQLView",
]
