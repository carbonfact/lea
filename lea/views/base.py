from __future__ import annotations

import abc
import dataclasses
import pathlib
import re

import lea


@dataclasses.dataclass
class View(abc.ABC):
    origin: pathlib.Path
    relative_path: pathlib.Path
    client: lea.clients.base.Client

    @abc.abstractclassmethod
    def path_suffixes(self) -> set[str]:
        ...

    @property
    def path(self):
        return self.origin.joinpath(self.relative_path)

    def __post_init__(self):
        if not isinstance(self.path, pathlib.Path):
            self.path = pathlib.Path(self.path)

    @property
    def key(self) -> tuple[str, ...]:
        """

        The key is a way to name the view. It is a tuple of strings that is constructed from the
        path where the view is located. The last element of the tuple is the name of the file
        without the extension. The rest of the elements are the folders that contain the file.

        """
        return tuple([*self.relative_path.parts[:-1], self.relative_path.name.split(".")[0]])

    @property
    def schema(self):
        return self.key[0]

    @property
    @abc.abstractmethod
    def dependent_view_keys(self) -> set[tuple[str, ...]]:
        """Return table references that the view depends on."""

    def yield_assertion_tests(self):
        # Unit tests in Python views are not handled yet
        if isinstance(self, lea.views.PythonView):
            return
            yield

        for field in self.fields:
            for tag in field.tags:
                if tag == "#NO_NULLS":
                    yield lea.views.InMemorySQLView(
                        key=(*self.key, field.name, "NO_NULLS"),
                        query=self.client.make_column_test_no_nulls(self, field.name),
                        client=self.client,
                    )

                elif tag == "#UNIQUE":
                    yield lea.views.InMemorySQLView(
                        key=(*self.key, field.name, "UNIQUE"),
                        query=self.client.make_column_test_unique(self, field.name),
                        client=self.client,
                    )

                elif unique_by := re.fullmatch("#UNIQUE_BY" + r"\((?P<by>.+)\)", tag):
                    by = unique_by.group("by")
                    yield lea.views.InMemorySQLView(
                        key=(*self.key, field.name, f"UNIQUE_BY({by})"),
                        query=self.client.make_column_test_unique_by(self, field.name, by),
                        client=self.client,
                    )

                elif set_ := re.fullmatch("#SET" + r"\{(?P<elements>\w+(?:,\s*\w+)*)\}", tag):
                    elements = {element.strip() for element in set_.group("elements").split(",")}
                    yield lea.views.InMemorySQLView(
                        key=(*self.key, field.name, "SET"),
                        query=self.client.make_column_test_set(self, field.name, elements),
                        client=self.client,
                    )

                else:
                    raise ValueError(f"Unhandled tag: {tag}")

    @abc.abstractmethod
    def with_context(self, table_reference_mapping: dict[str, str]) -> View:
        ...


@dataclasses.dataclass
class Field:
    name: str
    tags: set[str]
    description: str

    @property
    def is_unique(self):
        return "#UNIQUE" in self.tags
