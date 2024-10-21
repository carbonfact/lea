import pathlib
import pytest
from lea.table_ref import TableRef


@pytest.mark.parametrize("table_ref, expected",
    [
        pytest.param(table_ref, expected, id=str(table_ref))
        for table_ref, expected in [
            (TableRef("my_dataset", ("my_schema",), "my_table"), "my_dataset.my_schema.my_table"),
            (TableRef("my_dataset", (), "my_table"), "my_dataset.my_table"),
            (TableRef("my_dataset", ("my_schema", "my_subschema"), "my_table"), "my_dataset.my_schema.my_subschema.my_table"),
        ]
    ]
)
def test_str(table_ref, expected):
    assert str(table_ref) == expected


@pytest.mark.parametrize("table_ref, expected", [
    pytest.param(table_ref, expected, id=str(table_ref))
    for table_ref, expected in [
        (TableRef("my_dataset", ("my_schema",), "my_table"), "TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table')"),
        (TableRef("my_dataset", (), "my_table"), "TableRef(dataset='my_dataset', schema=(), name='my_table')"),
        (TableRef("my_dataset", ("my_schema", "my_subschema"), "my_table"), "TableRef(dataset='my_dataset', schema=('my_schema', 'my_subschema'), name='my_table')")
    ]
])
def test_repr(table_ref, expected):
    assert repr(table_ref) == expected


def test_from_path():
    path = pathlib.Path("my_dataset/my_schema/my_table")
    table_ref = TableRef.from_path(path)
    assert table_ref == TableRef("my_dataset", ("my_schema",), "my_table")
