from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
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
class ProductLookupItem:
    """Lookup entry used while mapping cart line items."""

    product_id: int
    sku: str
    title: str | None
    category: str | None
    unit_price_usd: Decimal | None
    discount_pct: Decimal | None


ProductLookup = dict[int, ProductLookupItem]


@dataclass(frozen=True)
class UserLookupItem:
    """Lookup entry used while enriching orders from mapped users."""

    customer_id: int
    country: str | None
    city: str | None
    email: str | None


UserLookup = dict[int, UserLookupItem]


@dataclass(frozen=True)
class MappedUsers:
    """Mapped users post injestion."""

    rows: list[StageRow] = field(default_factory=list)
    rejects: list[StageReject] = field(default_factory=list)
    user_lookup: UserLookup = field(default_factory=dict)


@dataclass(frozen=True)
class MappedProducts:
    """Mapped products post injestion."""

    rows: list[StageRow] = field(default_factory=list)
    rejects: list[StageReject] = field(default_factory=list)
    product_lookup: ProductLookup = field(default_factory=dict)


@dataclass(frozen=True)
class MappedCarts:
    """Mapped carts post injestion."""

    order_rows: list[StageRow] = field(default_factory=list)
    order_item_rows: list[StageRow] = field(default_factory=list)
    rejects: list[StageReject] = field(default_factory=list)


@dataclass(frozen=True)
class StageTableLoadResult:
    """Each table's database load summary after injestion."""

    table_name: str
    inserted_count: int
    duplicate_reject_count: int
    explicit_reject_count: int


__all__ = [
    "MappedCarts",
    "MappedProducts",
    "MappedUsers",
    "ProductLookup",
    "ProductLookupItem",
    "StageReject",
    "StageRow",
    "StageTableLoadResult",
    "UserLookup",
    "UserLookupItem",
]
