from __future__ import annotations

import pathlib

import sqlglot

from .base import View
from .dag import DAGOfViews
from .python import PythonView
from .sql import GenericSQLView, SQLView


def load_views(
    views_dir: pathlib.Path | str, sqlglot_dialect: sqlglot.dialects.Dialect | str
) -> list[View]:
    # Massage the inputs
    if isinstance(views_dir, str):
        views_dir = pathlib.Path(views_dir)
    if isinstance(sqlglot_dialect, str):
        sqlglot_dialect = sqlglot.dialects.Dialects(sqlglot_dialect)

    def _load_view_from_path(path, origin, sqlglot_dialect):
        relative_path = path.relative_to(origin)
        if path.suffix == ".py":
            return PythonView(origin, relative_path)
        if path.suffix == ".sql" or path.suffixes == [".sql", ".jinja"]:
            return SQLView(origin, relative_path, sqlglot_dialect=sqlglot_dialect)

    return [
        _load_view_from_path(path, origin=views_dir, sqlglot_dialect=sqlglot_dialect)
        for schema_dir in (d for d in views_dir.iterdir() if d.is_dir())
        for path in schema_dir.rglob("*")
        if not path.is_dir()
        and not path.name.startswith("_")
        and (path.suffix in {".py", ".sql"} or path.suffixes == [".sql", ".jinja"])
        and path.stat().st_size > 0
    ]


__all__ = [
    "load_views",
    "DAGOfViews",
    "View",
    "PythonView",
    "SQLView",
    "GenericSQLView",
]
