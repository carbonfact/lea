from __future__ import annotations

import pathlib

from .base import View
from .dag import DAGOfViews
from .python import PythonView
from .sql import GenericSQLView, SQLView


def load_views(views_dir: pathlib.Path | str) -> list[View]:
    if isinstance(views_dir, str):
        views_dir = pathlib.Path(views_dir)

    def _load_view_from_path(path, origin):
        relative_path = path.relative_to(origin)
        if path.suffix == ".py":
            return PythonView(origin, relative_path)
        if path.suffix == ".sql" or path.suffixes == [".sql", ".jinja"]:
            return SQLView(origin, relative_path)

    return [
        _load_view_from_path(path, origin=views_dir)
        for schema_dir in (d for d in views_dir.iterdir() if d.is_dir())
        for path in schema_dir.rglob("*")
        if not path.is_dir()
        and not path.name.startswith("_")
        and (path.suffix in {".py", ".sql"} or path.suffixes == [".sql", ".jinja"])
        and path.stat().st_size > 0
    ]



__all__ = ["load_views", "DAGOfViews", "View", "PythonView", "SQLView", "GenericSQLView"]
