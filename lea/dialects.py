from __future__ import annotations

import pathlib
import re
import textwrap

import jinja2
import sqlglot
import sqlglot.dialects

from lea.field import FieldTag
from lea.table_ref import TableRef


class SQLDialect:
    sqlglot_dialect: sqlglot.dialects.Dialects | None = None

    # Quack mode: name used when attaching this DB to DuckDB (e.g. "bq", "sf")
    quack_attached_name: str | None = None

    @staticmethod
    def parse_table_ref(table_ref: str) -> TableRef:
        raise NotImplementedError

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        raise NotImplementedError

    def quack_setup_sql(self, env: dict[str, str], dataset: str | None = None) -> list[str]:
        """Return SQL statements to install/load extension and attach native DB to DuckDB."""
        return []

    def format_table_ref_for_duckdb(self, table_ref: TableRef) -> str:
        """Format a native table ref as seen from DuckDB via the attached extension.

        >>> BigQueryDialect().format_table_ref_for_duckdb(
        ...     TableRef(dataset='my_dataset', schema=('my_schema',), name='my_table', project=None)
        ... )
        'bq.my_dataset.my_schema__my_table'

        """
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
        incremental_field_values_str = ", ".join(f"'{value}'" for value in incremental_field_values)
        for (
            dependency_without_wap_suffix,
            dependency_with_wap_suffix,
        ) in incremental_dependencies.items():
            dependency_without_wap_suffix_str = cls.format_table_ref(dependency_without_wap_suffix)
            dependency_with_wap_suffix_str = cls.format_table_ref(dependency_with_wap_suffix)
            code = re.sub(
                # We could use \b, but it doesn't work with backticks
                rf"(?<!\S){re.escape(dependency_with_wap_suffix_str)}(?=[,\s]|$)",
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


def load_assertion_test_template(tag: str) -> jinja2.Template:
    return jinja2.Template(
        (pathlib.Path(__file__).parent / "assertions" / f"{tag.lstrip('#')}.sql.jinja").read_text()
    )


class BigQueryDialect(SQLDialect):
    sqlglot_dialect = sqlglot.dialects.Dialects.BIGQUERY
    quack_attached_name = "bq"

    def quack_setup_sql(self, env: dict[str, str], dataset: str | None = None) -> list[str]:
        project = env["LEA_BQ_PROJECT_ID"]
        attach_str = f"project={project}"
        if dataset:
            attach_str += f" dataset={dataset}"
        return [
            "INSTALL bigquery FROM community;",
            "LOAD bigquery;",
            f"ATTACH '{attach_str}' AS {self.quack_attached_name} (TYPE bigquery, READ_ONLY);",
        ]

    def format_table_ref_for_duckdb(self, table_ref: TableRef) -> str:
        flat_name = "__".join([*table_ref.schema, table_ref.name])
        return f"{self.quack_attached_name}.{table_ref.dataset}.{flat_name}"

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
        TableRef(dataset='hubspot', schema=(), name='company', project='carbonfact-gsheet')

        """
        project, dataset, leftover = None, *tuple(table_ref.rsplit(".", 1))
        if "." in dataset:
            project, dataset = dataset.split(".", 1)
        *schema, name = tuple(re.split(r"(?<!_)__(?!_)", leftover))
        return TableRef(
            dataset=strip_quotes(dataset),
            schema=tuple([strip_quotes(s) for s in schema]),
            name=strip_quotes(name),
            project=strip_quotes(project) if project else None,
        )

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        table_ref_str = ""
        if table_ref.project:
            table_ref_str += f"{table_ref.project}."
        if table_ref.dataset:
            table_ref_str += f"{table_ref.dataset}."
        table_ref_str += f"{'__'.join([*table_ref.schema, table_ref.name])}"
        return table_ref_str

    @staticmethod
    def convert_table_ref_to_bigquery_table_reference(table_ref: TableRef, project: str):
        from google.cloud import bigquery

        return bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project=project,
                dataset_id=table_ref.dataset,  # type: ignore
            ),
            table_id=f"{'__'.join([*table_ref.schema, table_ref.name])}",
        )


class DuckDBDialect(SQLDialect):
    sqlglot_dialect = sqlglot.dialects.Dialects.DUCKDB

    @staticmethod
    def parse_table_ref(table_ref: str) -> TableRef:
        """
        Parses a DuckDB table reference string into a TableRef object.

        >>> DuckDBDialect.parse_table_ref("my_schema.my_table")
        TableRef(dataset=None, schema=('my_schema',), name='my_table', project=None)

        >>> DuckDBDialect.parse_table_ref("my_schema.my_subschema__my_table")
        TableRef(dataset=None, schema=('my_schema', 'my_subschema'), name='my_table', project=None)

        >>> DuckDBDialect.parse_table_ref("my_table")
        TableRef(dataset=None, schema=(), name='my_table', project=None)
        """
        if "." in table_ref:
            project, schema, leftover = None, *tuple(table_ref.rsplit(".", 1))
            *subschema, name = tuple(re.split(r"(?<!_)__(?!_)", leftover))

            return TableRef(
                dataset=None,
                schema=tuple([strip_quotes(schema), *[strip_quotes(ss) for ss in subschema]]),
                name=strip_quotes(name),
                project=strip_quotes(project) if project else None,
            )
        return TableRef(dataset=None, schema=(), name=table_ref, project=None)

    @staticmethod
    def format_table_ref(table_ref: TableRef) -> str:
        """
        Formats a TableRef object into a DuckDB table reference string.

        >>> DuckDBDialect.format_table_ref(TableRef(dataset=None, schema=('my_schema',), name='my_table', project=None))
        'my_schema.my_table'

        >>> DuckDBDialect.format_table_ref(TableRef(dataset=None, schema=('my_schema', 'my_subschema'), name='my_table', project=None))
        'my_schema.my_subschema__my_table'

        >>> DuckDBDialect.format_table_ref(TableRef(dataset=None, schema=(), name='my_table', project=None))
        'my_table'
        """
        if len(table_ref.schema) > 0:
            schema = table_ref.schema[0]
            if len(table_ref.schema) > 1:
                full_table_ref = f"{schema}.{'__'.join([*table_ref.schema[1:], table_ref.name])}"
            else:
                full_table_ref = f"{schema}.{table_ref.name}"
            return full_table_ref
        return table_ref.name

    @staticmethod
    def convert_table_ref_to_duckdb_table_reference(table_ref: TableRef) -> str:
        return DuckDBDialect.format_table_ref(table_ref)


def strip_quotes(x: str) -> str:
    return x.strip('"').strip("`")
