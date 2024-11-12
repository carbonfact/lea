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

    def __str__(self):
        return ".".join([self.dataset, *self.schema, self.name])

    @classmethod
    def from_path(cls, scripts_dir: pathlib.Path, relative_path: pathlib.Path) -> TableRef:
        parts = list(filter(None, relative_path.parts))
        *schema, filename = parts
        return cls(
            dataset=scripts_dir.name,
            schema=tuple(schema),
            # Remove the ex
            name=filename.split(".")[0],
        )

    def replace_dataset(self, dataset: str) -> TableRef:
        return dataclasses.replace(self, dataset=dataset)

    def add_audit_suffix(self) -> TableRef:
        return dataclasses.replace(self, name=f"{self.name}{AUDIT_TABLE_SUFFIX}")

    def remove_audit_suffix(self) -> TableRef:
        return dataclasses.replace(self, name=re.sub(rf"{AUDIT_TABLE_SUFFIX}$", "", self.name))
