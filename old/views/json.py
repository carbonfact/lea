from __future__ import annotations

from .base import View


class JSONView(View):
    @classmethod
    def path_suffixes(self):
        return {"json"}

    @property
    def dependent_view_keys(self):
        return set()

    def extract_comments(self, columns: list[str]):
        return {}

    def __repr__(self):
        return ".".join(self.key)

    def with_context(self, table_reference_mapping):
        return self

    @property
    def fields(self):
        return []  # TODO
