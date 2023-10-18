from __future__ import annotations

import ast
import importlib

from .base import View
from .sql import SQLView


class PythonView(View):
    @property
    def dependencies(self):
        def _dependencies():
            code = self.path.read_text()
            for node in ast.walk(ast.parse(code)):
                # pd.read_gbq
                try:
                    if (
                        isinstance(node, ast.Call)
                        and node.func.value.id == "pd"
                        and node.func.attr == "read_gbq"
                    ):
                        yield from SQLView.parse_dependencies(node.args[0].value)
                except AttributeError:
                    pass

                # .query
                try:
                    if isinstance(node, ast.Call) and node.func.attr.startswith("query"):
                        yield from SQLView.parse_dependencies(node.args[0].value)
                except AttributeError:
                    pass

        return set(_dependencies())

    @property
    def description(self):
        module_name = self.path.stem
        spec = importlib.util.spec_from_file_location(module_name, self.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.__doc__

    def extract_comments(self, columns: list[str]):
        return {}

    def __repr__(self):
        return ".".join(self.key)
