from __future__ import annotations

import dataclasses
import pathlib
import re

AUDIT_TABLE_SUFFIX = "___audit"


@dataclasses.dataclass(eq=True, frozen=True)
class TableRef:
    dataset: str | None
    schema: tuple[str, ...]
    name: str
    project: str | None
    # Whether the project was written in the code, rather than filled in with a default. It is
    # part of equality, so that the same table written with and without its project gives two
    # dependencies, which may be treated differently.
    project_is_explicit: bool = dataclasses.field(default=False, repr=False)

    def __str__(self):
        return ".".join(filter(None, [self.project, self.dataset, *self.schema, self.name]))

    @classmethod
    def from_path(
        cls, scripts_dir: pathlib.Path, relative_path: pathlib.Path, project_name: str | None
    ) -> TableRef:
        parts = list(filter(None, relative_path.parts))
        *schema, filename = parts
        return cls(
            dataset=scripts_dir.name,
            schema=tuple(schema),
            name=filename.split(".")[0],  # remove the extension
            project=project_name,
        )

    def replace_dataset(self, dataset: str | None) -> TableRef:
        return dataclasses.replace(self, dataset=dataset)

    def replace_project(self, project: str | None) -> TableRef:
        return dataclasses.replace(self, project=project)

    def as_local(self) -> TableRef:
        """Forget that the project was written, once the dependency is known to be local."""
        return dataclasses.replace(self, project_is_explicit=False)

    def is_external(self, project: str | None, dataset: str) -> bool:
        """Check if this dependency deliberately points outside of the scripts being run.

        This is the case when it is written with its project, and that project is not the
        scripts' project or its dataset is not the scripts' dataset. A dependency written without
        a project is always local, whatever its dataset, because scripts are often written against
        one dataset and built into another.

        """
        return self.project_is_explicit and (self.project != project or self.dataset != dataset)

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
