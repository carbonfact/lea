from __future__ import annotations

import pathlib
import re
import textwrap

import jinja2
import sqlglot
from google.cloud import bigquery

from lea.field import FieldTag
from lea.table_ref import TableRef


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

    @classmethod
    def add_dependency_filters(
        cls,
        code: str,
        incremental_field_name: str,
        incremental_field_values: set[str],
        dependencies_to_filter: set[TableRef],
    ) -> str:
        code = remove_comment_lines(code)
        incremental_field_values_str = ", ".join(f"'{value}'" for value in incremental_field_values)
        for dependency in dependencies_to_filter:
            dependency_str = cls.format_table_ref(dependency)
            code = re.sub(
                # We could use \b, but it doesn't work with backticks
                rf"(?<!\S){re.escape(dependency_str)}(?!\S)",
                f"(SELECT * FROM {dependency_str} WHERE {incremental_field_name} IN ({incremental_field_values_str}))",
                code,
            )
        return (
            "SELECT * FROM (\n"
            + textwrap.indent(code, prefix="    ")
            + f"\n)\nWHERE {incremental_field_name} IN ({incremental_field_values_str})"
        )

    @classmethod
    def handle_incremental_dependencies(
        cls,
        code: str,
        incremental_field_name: str,
        incremental_field_values: set[str],
        incremental_dependencies: dict[TableRef, TableRef],
    ) -> str:
        code = remove_comment_lines(code)
        incremental_field_values_str = ", ".join(f"'{value}'" for value in incremental_field_values)
        for (
            dependency_without_wap_suffix,
            dependency_with_wap_suffix,
        ) in incremental_dependencies.items():
            dependency_without_wap_suffix_str = cls.format_table_ref(dependency_without_wap_suffix)
            dependency_with_wap_suffix_str = cls.format_table_ref(dependency_with_wap_suffix)
            code = re.sub(
                # We could use \b, but it doesn't work with backticks
                rf"(?<!\S){re.escape(dependency_with_wap_suffix_str)}(?!\S)",
                f"""
                (
                    SELECT * FROM {dependency_with_wap_suffix_str}
                    WHERE {incremental_field_name} IN ({incremental_field_values_str})
                    UNION ALL
                    SELECT * FROM {dependency_without_wap_suffix_str}
                    WHERE {incremental_field_name} NOT IN ({incremental_field_values_str})
                )
                """,
                code,
            )
        return code


def remove_comment_lines(code: str) -> str:
    return "\n".join(line for line in code.split("\n") if not line.strip().startswith("--"))


def load_assertion_test_template(tag: str) -> jinja2.Template:
    return jinja2.Template(
        (pathlib.Path(__file__).parent / "assertions" / f"{tag.lstrip('#')}.sql.jinja").read_text()
    )


class BigQueryDialect(SQLDialect):
    sqlglot_dialect = sqlglot.dialects.Dialects.BIGQUERY

    @staticmethod
    def parse_table_ref(table_ref: str) -> TableRef:
        """

        >>> BigQueryDialect.parse_table_ref("my_dataset.my_schema__my_table")
        TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table', project=None)

        >>> BigQueryDialect.parse_table_ref("my_dataset.my_table")
        TableRef(dataset='my_dataset', schema=(), name='my_table', project=None)

        >>> BigQueryDialect.parse_table_ref("my_dataset.my_schema__my_table___audit")
        TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table___audit', project=None)

        >>> BigQueryDialect.parse_table_ref("my_project.my_dataset.my_schema__my_table___audit")
        TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table___audit', project='my_project')

        >>> BigQueryDialect.parse_table_ref("`carbonfact-gsheet`.hubspot.company")
        TableRef(dataset='hubspot', schema=(), name='company', project='`carbonfact-gsheet`')

        """
        project, dataset, leftover = None, *tuple(table_ref.rsplit(".", 1))
        if "." in dataset:
            project, dataset = dataset.split(".")
        *schema, name = tuple(re.split(r"(?<!_)__(?!_)", leftover))
        return TableRef(dataset=dataset, schema=tuple(schema), name=name, project=project)

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        table_ref_str = ""
        if table_ref.project:
            table_ref_str += f"`{table_ref.project}`."
        if table_ref.dataset:
            table_ref_str += f"{table_ref.dataset}."
        table_ref_str += f"{'__'.join([*table_ref.schema, table_ref.name])}"
        return table_ref_str

    @staticmethod
    def convert_table_ref_to_bigquery_table_reference(
        table_ref: TableRef, project: str
    ) -> bigquery.TableReference:
        return bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(project=project, dataset_id=table_ref.dataset),
            table_id=f"{'__'.join([*table_ref.schema, table_ref.name])}",
        )
