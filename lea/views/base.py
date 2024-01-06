from __future__ import annotations

import abc
import dataclasses
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
    def key(self):
        return tuple([*self.relative_path.parts[:-1], self.relative_path.name.split(".")[0]])

    @property
    def schema(self):
        return self.key[0]

    @property
    @abc.abstractmethod
    def dependencies(self) -> set[str]:
        ...
