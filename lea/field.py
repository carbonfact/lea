from __future__ import annotations

import dataclasses
import enum


@dataclasses.dataclass
class Field:
    name: str
    tags: set[FieldTag]
    description: str

    @property
    def is_unique(self):
        return FieldTag.UNIQUE in self.tags


class FieldTag(enum.StrEnum):
    NO_NULLS = "#NO_NULLS"
    UNIQUE = "#UNIQUE"
    UNIQUE_BY = "#UNIQUE_BY"
    SET = "#SET"
    INCREMENTAL = "#INCREMENTAL"
