from __future__ import annotations
import pathlib

import jinja2
import sqlglot

from .table_ref import TableRef
from .field import FieldTag


class SQLDialect:
    sqlglot_dialect: sqlglot.dialects.Dialects | None = None

    @staticmethod
    def parse_table_ref(table_ref: str) -> TableRef:
        raise NotImplementedError

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        raise NotImplementedError

    def make_column_test_unique(self, table_ref: TableRef, field_name: str) -> str:
        table_ref_str = self.format_table_ref(table_ref)
        return load_assertion_test_template(FieldTag.UNIQUE).render(
            table=table_ref_str, column=field_name
        )

    def make_column_test_unique_by(self, table_ref: TableRef, field_name: str, by: str) -> str:
        table_ref_str = self.format_table_ref(table_ref)
        return load_assertion_test_template(FieldTag.UNIQUE_BY).render(
            table=table_ref_str,
            column=field_name,
            by=by,
        )

    def make_column_test_no_nulls(self, table_ref: TableRef, field_name: str) -> str:
        table_ref_str = self.format_table_ref(table_ref)
        return load_assertion_test_template(FieldTag.NO_NULLS).render(
            table=table_ref_str, column=field_name
        )

    def make_column_test_set(self, table_ref: TableRef, field_name: str, elements: set[str]) -> str:
        table_ref_str = self.format_table_ref(table_ref)
        return load_assertion_test_template(FieldTag.SET).render(
            table=table_ref_str,
            column=field_name,
            elements=elements,
        )

    def make_incremental(self, code: str, field_name: str, field_values: set[str], dependencies_to_filter: set[TableRef]) -> str:
        field_values_str = ", ".join(f"'{value}'" for value in field_values)
        for dependency in dependencies_to_filter:
            dependency_str = self.format_table_ref(dependency)
            code = code.replace(
                dependency_str,
                f"(SELECT * FROM {dependency_str} WHERE {field_name} IN ({field_values_str}))"
            )
        code = f"SELECT * FROM ({code}) WHERE {field_name} IN ({field_values_str})"
        return code


def load_assertion_test_template(tag: str) -> jinja2.Template:
    return jinja2.Template(
        (
            pathlib.Path(__file__).parent / "assertions" / f"{tag.lstrip('#')}.sql.jinja"
        ).read_text()
    )


class BigQueryDialect(SQLDialect):
    sqlglot_dialect = sqlglot.dialects.Dialects.BIGQUERY

    @staticmethod
    def parse_table_ref(table_ref: str) -> TableRef:
        dataset, leftover = tuple(table_ref.rsplit(".", 1))
        *schema, name = tuple(leftover.split("__"))
        return TableRef(dataset=dataset, schema=tuple(schema), name=name)

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        table_ref_str = ""
        if table_ref.dataset:
            table_ref_str += f"{table_ref.dataset}."
        table_ref_str += f"{'__'.join([*table_ref.schema, table_ref.name])}"
        return table_ref_str
