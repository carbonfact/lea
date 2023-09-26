from __future__ import annotations

import abc
import dataclasses
import itertools
import pathlib


@dataclasses.dataclass
class View(abc.ABC):
    origin: pathlib.Path
    relative_path: pathlib.Path

    @property
    def path(self):
        return self.origin.joinpath(self.relative_path)

    def __post_init__(self):
        if not isinstance(self.path, pathlib.Path):
            self.path = pathlib.Path(self.path)

    @property
    def schema(self):
        return self.relative_path.parts[0]

    @property
    def name(self):
        name_parts = itertools.chain(
            self.relative_path.parts[1:-1], [self.relative_path.name.split(".")[0]]
        )
        return "__".join(name_parts)

    @property
    def dunder_name(self):
        return f"{self.schema}__{self.name}"

    @property
    @abc.abstractmethod
    def dependencies(self) -> set[str]:
        ...
