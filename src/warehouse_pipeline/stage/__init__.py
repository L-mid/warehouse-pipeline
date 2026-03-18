from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StageRow:
    """A mapped row ready to be written to a staging work table."""

    table_name: str
    source_ref: int
    raw_payload: Mapping[str, Any]
    values: Mapping[str, Any]


@dataclass(frozen=True)
class StageReject:
    """A row rejected during stage mapping before database load."""

    table_name: str
    source_ref: int
    raw_payload: Mapping[str, Any]
    reason_code: str
    reason_detail: str


@dataclass(frozen=True)
class MappedSquareOrders:
    order_rows: list[StageRow] = field(default_factory=list)
    order_line_rows: list[StageRow] = field(default_factory=list)
    tender_rows: list[StageRow] = field(default_factory=list)
    rejects: list[StageReject] = field(default_factory=list)


@dataclass(frozen=True)
class StageTableLoadResult:
    table_name: str
    inserted_count: int
    duplicate_reject_count: int
    explicit_reject_count: int


__all__ = [
    "MappedSquareOrders",
    "StageReject",
    "StageRow",
    "StageTableLoadResult",
]
