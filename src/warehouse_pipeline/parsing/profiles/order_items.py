from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from warehouse_pipeline.parsing.adapter import adapt_row
from warehouse_pipeline.parsing.primitives import (
    parse_int,
    parse_numeric_12_2,
    parse_required_text,
    any_upper
)
from warehouse_pipeline.parsing.schema import FieldSpec, RowParser
from warehouse_pipeline.parsing.types import RejectRow


def _default_discount_usd_0(canon: dict[str, Any]) -> dict[str, Any]:
    """
    Defaults `discount_usd` to "0" (str) in the case of missing/`None`/empty.
    Otherwise returns value as is.
    """
    disc = canon.get("discount_usd")
    if disc is None or str(disc).strip() == "":
        disc = "0"
        canon["discount_usd"] = disc 
    return canon


def canonicalize_order_items_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonicalization/derivation step for order items shaped data.
    - `discount_usd` is defaulted to 0 if missing.
    
    Input is expected to already be transformed into canonical keys via `adapt_row` aliases.
    Output keys should align with `_CUSTOMERS_KNOWN`.
    """

    canon: dict[str, Any] = dict(raw)           # canonicalization step

    canon = _default_discount_usd_0(canon)      # default missing discount instances to 0 (the DB column is `NOT NULL DEFAULT 0`).

    return {
        "order_id": canon.get("order_id"),
        "line_id": canon.get("line_id"),
        "sku": canon.get("sku"),
        "qty": canon.get("qty"),
        "unit_price_usd": canon.get("unit_price_usd"),
        "discount_usd": canon.get("discount_usd"),
    }


# Allowed input keys, how they map to canonical keys
_ORDER_ITEMS_INPUT_ALIASES: dict[str, str] = {
    "order_id": "order_id",
    "line_id": "line_id",
    "sku": "sku",
    "qty": "qty",
    "unit_price_usd": "unit_price_usd",
    "discount_usd": "discount_usd",
}

# all known expected fields for `orders_items.csv`
_ORDER_ITEMS_KNOWN = set(_ORDER_ITEMS_INPUT_ALIASES.values())


order_items_parser = RowParser(
    known_fields=_ORDER_ITEMS_KNOWN,    # catches bugs where canonicalization adds extra keys
    reject_unknown_fields=True, 
    default_text_transform=None,
    fields=[
        FieldSpec("order_id", lambda r: r.get("order_id"), lambda v: parse_required_text(v, field="order_id"), True),
        FieldSpec("line_id", lambda r: r.get("line_id"), lambda v: parse_int(v, field="line_id"), True),

        # sku casing overridden to upper
        FieldSpec("sku", lambda r: r.get("sku"), lambda v: parse_required_text(v, field="sku"), True, transform=any_upper),
        FieldSpec("qty", lambda r: r.get("qty"), lambda v: parse_int(v, field="qty"), True),
        FieldSpec("unit_price_usd", lambda r: r.get("unit_price_usd"), lambda v: parse_numeric_12_2(v, field="unit_price_usd"), True),
        FieldSpec("discount_usd", lambda r: r.get("discount_usd"), lambda v: parse_numeric_12_2(v, field="discount_usd"), True),
    ],
)


def parse_order_item_row(raw: Mapping[str, Any], *, source_row: int) -> object:
    """Parse a single order item's row."""
    adapted = adapt_row(
        raw,
        aliases=_ORDER_ITEMS_INPUT_ALIASES,
        source_row=source_row,
        raw_payload={"raw": dict(raw)},
        reject_unknown_input_fields=True,
    )
    if isinstance(adapted, RejectRow):
        return adapted

    # specific normalizations for order items, pre-parsing
    canon = canonicalize_order_items_row(adapted)
    # ensure only expected keys to get parsed actually get parsed 
    # (`None` caught by parser if invalid for that field)
    canon = {k: canon.get(k) for k in _ORDER_ITEMS_KNOWN}

    return order_items_parser.parse(
        canon,
        source_row=source_row,
        raw_payload={"raw": dict(raw), "canonical": canon},
    )


@dataclass(frozen=True)
class FunctionRowParser:
    """Return `parse_order_item_row` in an object, for imports."""
    fn: Any

    def parse(self, raw: Mapping[str, Any], *, source_row: int) -> object:
        """Return the provided parser's outputs."""
        return self.fn(raw, source_row=source_row)
    
# import
ORDER_ITEMS_PARSER = FunctionRowParser(parse_order_item_row)

