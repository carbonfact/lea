from __future__ import annotations

import abc
import importlib
import pathlib
import re

import jinja2
import pandas as pd

from lea import views


class AssertionTag:
    NO_NULLS = "@NO_NULLS"
    UNIQUE = "@UNIQUE"
    UNIQUE_BY = "@UNIQUE_BY"
    SET = "@SET"


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
    def _key_to_reference(self, view_key: tuple[str]) -> str:
        ...

    @abc.abstractmethod
    def _reference_to_key(self, table_reference: str) -> tuple[str]:
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
    def delete_table_reference(self, table_reference: str):
        ...

    @abc.abstractmethod
    def list_tables(self) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def list_columns(self) -> pd.DataFrame:
        ...

    def make_column_test_unique(self, view: lea.views.View, column: str) -> str:
        return self.load_assertion_test_template(AssertionTag.UNIQUE).render(
            table=self._key_to_reference(view.key), column=column
        )

    def make_column_test_unique_by(self, view: lea.views.View, column: str, by: str) -> str:
        return self.load_assertion_test_template(AssertionTag.UNIQUE_BY).render(
            table=self._key_to_reference(view.key), column=column, by=by
        )

    def make_column_test_no_nulls(self, view: lea.views.View, column: str) -> str:
        return self.load_assertion_test_template(AssertionTag.NO_NULLS).render(
            table=self._key_to_reference(view.key), column=column
        )

    def make_column_test_set(self, view: lea.views.View, column: str, elements: set[str]) -> str:
        schema, *leftover = view.key
        return self.load_assertion_test_template(AssertionTag.SET).render(
            table=self._key_to_reference(view.key), column=column, elements=elements
        )

    def load_assertion_test_template(self, tag: str) -> jinja2.Template:
        return jinja2.Template(
            (
                pathlib.Path(__file__).parent / "assertions" / f"{tag.lstrip('@')}.sql.jinja"
            ).read_text()
        )

    def discover_assertion_tests(self, view, view_columns):
        # Unit tests in Python views are not handled yet
        if isinstance(view, views.PythonView):
            return
            yield

        column_comments = view.extract_comments(columns=view_columns)

        for column, comment_block in column_comments.items():
            for comment in comment_block:
                if "@" not in comment.text:
                    continue
                if comment.text == AssertionTag.NO_NULLS:
                    yield views.GenericSQLView(
                        schema="tests",
                        name=f"{view}.{column}{AssertionTag.NO_NULLS}",
                        query=self.make_column_test_no_nulls(view, column),
                        sqlglot_dialect=self.sqlglot_dialect,
                    )
                elif comment.text == AssertionTag.UNIQUE:
                    yield views.GenericSQLView(
                        schema="tests",
                        name=f"{view}.{column}{AssertionTag.UNIQUE}",
                        query=self.make_column_test_unique(view, column),
                        sqlglot_dialect=self.sqlglot_dialect,
                    )
                elif unique_by := re.fullmatch(
                    rf"{AssertionTag.UNIQUE_BY}\((?P<by>.+)\)", comment.text
                ):
                    by = unique_by.group("by")
                    yield views.GenericSQLView(
                        schema="tests",
                        name=f"{view}.{column}{AssertionTag.UNIQUE_BY}{by}",
                        query=self.make_column_test_unique_by(view, column, by),
                        sqlglot_dialect=self.sqlglot_dialect,
                    )
                elif set_ := re.fullmatch(
                    AssertionTag.SET + r"\{(?P<elements>\w+(?:,\s*\w+)*)\}", comment.text
                ):
                    elements = {element.strip() for element in set_.group("elements").split(",")}
                    yield views.GenericSQLView(
                        schema="tests",
                        name=f"{view}.{column}{AssertionTag.SET}",
                        query=self.make_column_test_set(view, column, elements),
                        sqlglot_dialect=self.sqlglot_dialect,
                    )
                else:
                    raise ValueError(f"Unhandled tag: {comment.text}")

    @abc.abstractmethod
    def teardown(self):
        ...
