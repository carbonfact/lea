from __future__ import annotations

import dataclasses
import pathlib
import re

AUDIT_TABLE_SUFFIX = "___audit"


@dataclasses.dataclass(eq=True, frozen=True)
class TableRef:
    dataset: str
    schema: tuple[str, ...]
    name: str
    project: str | None

    def __str__(self):
        return ".".join(filter(None, [self.project, self.dataset, *self.schema, self.name]))

    @classmethod
    def from_path(
        cls, scripts_dir: pathlib.Path, relative_path: pathlib.Path, project_name: str
    ) -> TableRef:
        parts = list(filter(None, relative_path.parts))
        *schema, filename = parts
        return cls(
            dataset=scripts_dir.name,
            schema=tuple(schema),
            name=filename.split(".")[0],  # remove the extension
            project=project_name,
        )

    def replace_dataset(self, dataset: str) -> TableRef:
        return dataclasses.replace(self, dataset=dataset)

    def replace_project(self, project: str) -> TableRef:
        return dataclasses.replace(self, project=project)

    def add_audit_suffix(self) -> TableRef:
        if self.is_audit_table:
            return self
        return dataclasses.replace(self, name=f"{self.name}{AUDIT_TABLE_SUFFIX}")

    def remove_audit_suffix(self) -> TableRef:
        if self.is_audit_table:
            return dataclasses.replace(self, name=re.sub(rf"{AUDIT_TABLE_SUFFIX}$", "", self.name))
        return self

    @property
    def is_audit_table(self) -> bool:
        return self.name.endswith(AUDIT_TABLE_SUFFIX)

    @property
    def is_test(self) -> bool:
        return len(self.schema) > 0 and self.schema[0] == "tests"
