import sqlglot
from .table_ref import TableRef


class BigQueryDialect:
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


SQLDialect = BigQueryDialect
