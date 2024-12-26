from __future__ import annotations

import dataclasses
import enum


@dataclasses.dataclass(frozen=True)
class Field:
    name: str
    tags: set[FieldTag] = dataclasses.field(default_factory=set)
    description: str | None = None

    @property
    def is_unique(self):
        return FieldTag.UNIQUE in self.tags


class FieldTag(enum.StrEnum):
    NO_NULLS = "#NO_NULLS"
    UNIQUE = "#UNIQUE"
    UNIQUE_BY = "#UNIQUE_BY"
    SET = "#SET"
    INCREMENTAL = "#INCREMENTAL"
