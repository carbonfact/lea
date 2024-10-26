from __future__ import annotations

import dataclasses
import pathlib


@dataclasses.dataclass(eq=True, frozen=True)
class TableRef:
    dataset: str
    schema: tuple[str, ...]
    name: str

    def __str__(self):
        return ".".join([self.dataset, *self.schema, self.name])

    @classmethod
    def from_path(cls, dataset_dir: pathlib.Path, relative_path: pathlib.Path) -> TableRef:
        parts = list(filter(None, relative_path.parts))
        *schema, filename = parts
        return cls(
            dataset=dataset_dir.name,
            schema=tuple(schema),
            # Remove the ex
            name=filename.split(".")[0]
        )

    def replace_dataset(self, dataset: str) -> TableRef:
        return dataclasses.replace(self, dataset=dataset)

    def add_wap_suffix(self) -> TableRef:
        return dataclasses.replace(self, name=f"{self.name}__audit")

    def remove_wap_suffix(self) -> TableRef:
        return dataclasses.replace(self, name=self.name.rsplit("__", 1)[0])
