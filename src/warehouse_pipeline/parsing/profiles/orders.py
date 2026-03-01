from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from warehouse_pipeline.parsing.adapter import adapt_row
from warehouse_pipeline.parsing.primitives import (
    parse_numeric_12_2,
    parse_required_text,
    parse_timestamptz_iso,
    text_lower,
    any_upper
)
from warehouse_pipeline.parsing.schema import FieldSpec, RowParser
from warehouse_pipeline.parsing.types import RejectRow


def canonicalize_orders_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonicalization/derivation step for order-shaped data.
    - None
    
    Input is expected to already be transformed into canonical keys via `adapt_row` aliases.
    Output keys should align with `_CUSTOMERS_KNOWN`.
    """

    canon: dict[str, Any] = dict(raw)           # canonicalization step

    return {
        "order_id": canon.get("order_id"),
        "customer_id": canon.get("customer_id"),
        "order_ts": canon.get("order_ts"),
        "country": canon.get("country"),
        "status": canon.get("status"),
        "total_usd": canon.get("total_usd"),
    }


# Allowed input keys, how they map to canonical keys
_ORDERS_INPUT_ALIASES: dict[str, str] = {
    "order_id": "order_id",
    "customer_id": "customer_id",
    "order_ts": "order_ts",
    "country": "country",
    "status": "status",
    "total_usd": "total_usd",
}

# all known expected fields for `orders.csv`
_ORDERS_KNOWN = set(_ORDERS_INPUT_ALIASES.values())


orders_parser = RowParser(
    known_fields=_ORDERS_KNOWN,
    reject_unknown_fields=True,         # catches bugs where canonicalization adds extra keys
    default_text_transform=text_lower,  # lower all string fields by default
    fields=[
        FieldSpec("order_id", lambda r: r.get("order_id"), lambda v: parse_required_text(v, field="order_id"), True),
        FieldSpec("customer_id", lambda r: r.get("customer_id"), lambda v: parse_required_text(v, field="customer_id"), True),
        FieldSpec("order_ts", lambda r: r.get("order_ts"), lambda v: parse_timestamptz_iso(v, field="order_ts"), True),
        
        # country casing is overriden to upper
        FieldSpec("country", lambda r: r.get("country"), lambda v: parse_required_text(v, field="country"), True, transform=any_upper),
        FieldSpec("status", lambda r: r.get("status"), lambda v: parse_required_text(v, field="status"), True),
        FieldSpec("total_usd", lambda r: r.get("total_usd"), lambda v: parse_numeric_12_2(v, field="total_usd"), True),
    ],
)


def parse_order_row(raw: Mapping[str, Any], *, source_row: int) -> object:
    """
    Parse a single order's row.
    """

    adapted = adapt_row(
        raw,
        aliases=_ORDERS_INPUT_ALIASES,
        source_row=source_row,
        raw_payload={"raw": dict(raw)},
        reject_unknown_input_fields=True,
    )
    if isinstance(adapted, RejectRow):
        return adapted

    # specific normalizations for orders, pre-parsing
    canon = canonicalize_orders_row(adapted)
    # ensure only expected keys to get parsed actually get parsed 
    # (`None` caught by parser if invalid for that field)
    canon = {k: canon.get(k) for k in _ORDERS_KNOWN}

    return orders_parser.parse(
        canon,
        source_row=source_row,
        raw_payload={"raw": dict(raw), "canonical": canon},
    )

@dataclass(frozen=True)
class FunctionRowParser:
    """Return `parse_order_row` in an object, for imports."""
    fn: Any # must be callable

    def parse(self, raw: Mapping[str, Any], *, source_row: int) -> object:
        """Return the provided parser's outputs."""
        return self.fn(raw, source_row=source_row)

# import
ORDERS_PARSER = FunctionRowParser(parse_order_row)