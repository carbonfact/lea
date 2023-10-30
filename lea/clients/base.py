from __future__ import annotations

import abc
import importlib

import pandas as pd

from lea import views


class Client(abc.ABC):
    """

    This is the base class for all clients. It defines the interface that all clients must
    implement. It is not meant to be used directly. Instead, use one of the subclasses.

    """

    def prepare(self):
        ...

    @abc.abstractproperty
    def sqlglot_dialect(self):
        ...

    @abc.abstractmethod
    def _make_view_path(self, view: views.View) -> str:
        ...

    @abc.abstractmethod
    def _create_sql_view(self, view: views.SQLView):
        ...

    @abc.abstractmethod
    def _create_python_view(self, view: views.PythonView):
        ...

    def create(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._create_sql_view(view)
        elif isinstance(view, views.PythonView):
            return self._create_python_view(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def _load_sql_view(self, view: views.SQLView):
        ...

    def _load_python_view(self, view: views.PythonView):
        module_name = view.path.stem
        spec = importlib.util.spec_from_file_location(module_name, view.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Step 2: Retrieve the variable from the module's namespace
        dataframe = getattr(module, view.key[1], None)  # HACK
        if dataframe is None:
            raise ValueError(f"Could not find variable {view.key[1]} in {view.path}")
        return dataframe

    def load(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._load_sql_view(view=view)
        elif isinstance(view, views.PythonView):
            return self._load_python_view(view=view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def delete_view(self, view: views.View):
        ...

    @abc.abstractmethod
    def list_existing_view_names(self) -> list[tuple[str, str]]:
        ...

    @abc.abstractmethod
    def get_tables(self, schema: str) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def get_columns(self, schema: str) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def make_test_unique_column(self, view: views.View, column: str) -> str:
        ...

    def yield_unit_tests(self, view, view_columns):
        # Unit tests in Python views are not handled yet
        if isinstance(view, views.PythonView):
            return
            yield

        column_comments = view.extract_comments(columns=view_columns)

        for column, comment_block in column_comments.items():
            for comment in comment_block:
                if "@" not in comment.text:
                    continue
                if comment.text == "@UNIQUE":
                    yield views.GenericSQLView(
                        schema="tests",
                        name=f"{view}.{column}@UNIQUE",
                        query=self.make_test_unique_column(view, column),
                        sqlglot_dialect=self.sqlglot_dialect,
                    )
                else:
                    raise ValueError(f"Unhandled tag: {comment.text}")

    @abc.abstractmethod
    def teardown(self):
        ...
