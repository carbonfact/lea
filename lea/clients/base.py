from __future__ import annotations

import abc
import importlib
import textwrap
import typing

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
    def _create_sql(self, view: views.SQLView):
        ...

    @abc.abstractmethod
    def _create_python(self, view: views.PythonView):
        ...

    def create(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._create_sql(view)
        elif isinstance(view, views.PythonView):
            return self._create_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def _load_sql(self, view: views.SQLView):
        ...

    def _load_python(self, view: views.PythonView):

        module_name = view.path.stem
        spec = importlib.util.spec_from_file_location(module_name, view.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Step 2: Retrieve the variable from the module's namespace
        dataframe = getattr(module, view.name, None)
        if dataframe is None:
            raise ValueError(f"Could not find variable {view.name} in {view.path}")
        return dataframe

    def load(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._load_sql(view)
        elif isinstance(view, views.PythonView):
            return self._load_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def delete_view(self, view: views.View):
        ...

    @abc.abstractmethod
    def list_existing_view_names(self) -> list[tuple[str, str]]:
        ...

    @abc.abstractmethod
    def get_columns(self, schema: str) -> pd.DataFrame:
        ...

    def get_diff_summary(self, origin: str, destination: str) -> pd.DataFrame:

        origin_columns = set(map(tuple, self.get_columns(origin)[["table", "column"]].values.tolist()))
        destination_columns = set(map(tuple, self.get_columns(destination)[["table", "column"]].values.tolist()))

        return pd.DataFrame(
            [
                {
                    "table": table,
                    "column": None,
                    "diff_kind": "ADDED",
                }
                for table in {t for t, _ in origin_columns} -  {t for t, _ in destination_columns}
            ] +
            [
                {
                    "table": table,
                    "column": column,
                    "diff_kind": "ADDED",
                }
                for table, column in origin_columns - destination_columns
            ] +
            [
                {
                    "table": table,
                    "column": None,
                    "diff_kind": "REMOVED",
                }
                for table in {t for t, _ in destination_columns } -  {t for t, _ in origin_columns}
            ] +
            [
                {
                    "table": table,
                    "column": column,
                    "diff_kind": "REMOVED",
                }
                for table, column in destination_columns - origin_columns
            ]
        )


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
                        name=f"{view.schema}.{view.name}.{column}@UNIQUE",
                        query=self.make_test_unique_column(view, column),
                    )
                else:
                    raise ValueError(f"Unhandled tag: {comment.text}")
