from __future__ import annotations

from .base import View


class JSONView(View):
    @classmethod
    def path_suffixes(self):
        return {"json"}

    def rename_table_references(self, table_reference_mapping: dict[str, str]):
        return self

    @property
    def dependencies(self):
        return set()

    def extract_comments(self, columns: list[str]):
        return {}

    def __repr__(self):
        return ".".join(self.key)
