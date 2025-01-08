from __future__ import annotations

import pathlib

import pytest

from lea.table_ref import TableRef


@pytest.mark.parametrize(
    "table_ref, expected",
    [
        pytest.param(table_ref, expected, id=str(table_ref))
        for table_ref, expected in [
            (
                TableRef("my_dataset", ("my_schema",), "my_table", "my_project"),
                "my_project.my_dataset.my_schema.my_table",
            ),
            (
                TableRef("my_dataset", (), "my_table", "my_project"),
                "my_project.my_dataset.my_table",
            ),
            (
                TableRef("my_dataset", ("my_schema", "my_subschema"), "my_table", "my_project"),
                "my_project.my_dataset.my_schema.my_subschema.my_table",
            ),
        ]
    ],
)
def test_str(table_ref, expected):
    assert str(table_ref) == expected


@pytest.mark.parametrize(
    "table_ref, expected",
    [
        pytest.param(table_ref, expected, id=str(table_ref))
        for table_ref, expected in [
            (
                TableRef("my_dataset", ("my_schema",), "my_table", None),
                "TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table', project=None)",
            ),
            (
                TableRef("my_dataset", (), "my_table", None),
                "TableRef(dataset='my_dataset', schema=(), name='my_table', project=None)",
            ),
            (
                TableRef("my_dataset", ("my_schema", "my_subschema"), "my_table", "my_project"),
                "TableRef(dataset='my_dataset', schema=('my_schema', 'my_subschema'), name='my_table', project='my_project')",
            ),
        ]
    ],
)
def test_repr(table_ref, expected):
    assert repr(table_ref) == expected


def test_from_path():
    scripts_dir = pathlib.Path("my_dataset")
    relative_path = pathlib.Path("my_schema/my_table.sql")
    table_ref = TableRef.from_path(scripts_dir, relative_path, "my_project")
    assert table_ref == TableRef("my_dataset", ("my_schema",), "my_table", "my_project")
