import ast
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
                    if isinstance(node, ast.Call) and node.func.attr.startswith(
                        "query"
                    ):
                        yield from SQLView.parse_dependencies(node.args[0].value)
                except AttributeError:
                    pass

        return set(_dependencies())
