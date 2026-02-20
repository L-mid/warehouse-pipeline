from __future__ import annotations

from typing import Any, Mapping

from warehouse_pipeline.parsing.adapter import adapt_row
from warehouse_pipeline.parsing.primitives import (
    parse_date_yyyy_mm_dd,
    parse_int,
    parse_numeric_12_2,
    parse_optional_text,
    parse_required_text,
)
from warehouse_pipeline.parsing.schema import FieldSpec, RowParser
from warehouse_pipeline.parsing.types import RejectRow


def canonicalize_retail_transaction_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonicalization/derivation step for `retail_transactions.csv`.

    Minimal for now: keys are already canonical.
    """
    return {
        "date": raw.get("date"),
        "week": raw.get("week"),
        "sku": raw.get("sku"),
        "product_category": raw.get("product_category"),
        "gender": raw.get("gender"),
        "marketplace": raw.get("marketplace"),
        "fulfillment": raw.get("fulfillment"),
        "color": raw.get("color"),
        "size": raw.get("size"),
        "list_price": raw.get("list_price"),
        "discount_pct": raw.get("discount_pct"),
        "promo_type": raw.get("promo_type"),
        "ad_spend": raw.get("ad_spend"),
        "impressions": raw.get("impressions"),
        "clicks": raw.get("clicks"),
        "cvr": raw.get("cvr"),
        "units_sold": raw.get("units_sold"),
        "revenue": raw.get("revenue"),
        "rating": raw.get("rating"),
        "reviews": raw.get("reviews"),
        "competitor_price_index": raw.get("competitor_price_index"),
        "stock_on_hand": raw.get("stock_on_hand"),
        "stockout_flag": raw.get("stockout_flag"),
        "holiday_flag": raw.get("holiday_flag"),
    }

# Allowed input keys, how they map to canonical keys
_RETAIL_INPUT_ALIASES: dict[str, str] = {
    "date": "date",
    "week": "week",
    "sku": "sku",
    "product_category": "product_category",
    "gender": "gender",
    "marketplace": "marketplace",
    "fulfillment": "fulfillment",
    "color": "color",
    "size": "size",
    "list_price": "list_price",
    "discount_pct": "discount_pct",
    "promo_type": "promo_type",
    "ad_spend": "ad_spend",
    "impressions": "impressions",
    "clicks": "clicks",
    "cvr": "cvr",
    "units_sold": "units_sold",
    "revenue": "revenue",
    "rating": "rating",
    "reviews": "reviews",
    "competitor_price_index": "competitor_price_index",
    "stock_on_hand": "stock_on_hand",
    "stockout_flag": "stockout_flag",
    "holiday_flag": "holiday_flag",
}

# all known expected fields for `retail_transactions.csv`
_RETAIL_KNOWN = set(_RETAIL_INPUT_ALIASES.values())

retail_transactions_parser = RowParser(
    known_fields=_RETAIL_KNOWN,
    reject_unknown_fields=True,  # catches bugs where canonicalization adds extra keys
    fields=[
        FieldSpec("date", lambda r: r.get("date"), lambda v: parse_date_yyyy_mm_dd(v, field="date"), True),
        FieldSpec("week", lambda r: r.get("week"), lambda v: parse_int(v, field="week"), True),
        FieldSpec("sku", lambda r: r.get("sku"), lambda v: parse_required_text(v, field="sku"), True),
        FieldSpec("product_category", lambda r: r.get("product_category"), lambda v: parse_required_text(v, field="product_category"), True),
        FieldSpec("gender", lambda r: r.get("gender"), lambda v: parse_required_text(v, field="gender"), True),
        FieldSpec("marketplace", lambda r: r.get("marketplace"), lambda v: parse_required_text(v, field="marketplace"), True),
        FieldSpec("fulfillment", lambda r: r.get("fulfillment"), lambda v: parse_required_text(v, field="fulfillment"), True),

        FieldSpec("color", lambda r: r.get("color"), parse_optional_text, False),
        FieldSpec("size", lambda r: r.get("size"), parse_optional_text, False),

        # numeric(12,2) quantization: parsing all numerics via numeric(12,2).
        # this will round, ex: cvr=0.0329 to 0.03, competitor_price_index=0.961 to 0.96.
        FieldSpec("list_price", lambda r: r.get("list_price"), lambda v: parse_numeric_12_2(v, field="list_price"), True),
        FieldSpec("discount_pct", lambda r: r.get("discount_pct"), lambda v: parse_numeric_12_2(v, field="discount_pct"), True),
        FieldSpec("promo_type", lambda r: r.get("promo_type"), lambda v: parse_required_text(v, field="promo_type"), True),
        FieldSpec("ad_spend", lambda r: r.get("ad_spend"), lambda v: parse_numeric_12_2(v, field="ad_spend"), True),

        FieldSpec("impressions", lambda r: r.get("impressions"), lambda v: parse_int(v, field="impressions"), True),
        FieldSpec("clicks", lambda r: r.get("clicks"), lambda v: parse_int(v, field="clicks"), True),
        FieldSpec("cvr", lambda r: r.get("cvr"), lambda v: parse_numeric_12_2(v, field="cvr"), True),

        FieldSpec("units_sold", lambda r: r.get("units_sold"), lambda v: parse_int(v, field="units_sold"), True),
        FieldSpec("revenue", lambda r: r.get("revenue"), lambda v: parse_numeric_12_2(v, field="revenue"), True),

        FieldSpec("rating", lambda r: r.get("rating"), lambda v: parse_numeric_12_2(v, field="rating"), True),
        FieldSpec("reviews", lambda r: r.get("reviews"), lambda v: parse_int(v, field="reviews"), True),

        FieldSpec("competitor_price_index", lambda r: r.get("competitor_price_index"), lambda v: parse_numeric_12_2(v, field="competitor_price_index"), True),
        FieldSpec("stock_on_hand", lambda r: r.get("stock_on_hand"), lambda v: parse_int(v, field="stock_on_hand"), True),
        FieldSpec("stockout_flag", lambda r: r.get("stockout_flag"), lambda v: parse_int(v, field="stockout_flag"), True),
        FieldSpec("holiday_flag", lambda r: r.get("holiday_flag"), lambda v: parse_int(v, field="holiday_flag"), True),
    ],
)


def parse_retail_transaction_row(raw: Mapping[str, Any], *, source_row: int) -> object:
    """Parse a single order's row."""
    adapted = adapt_row(
        raw,
        aliases=_RETAIL_INPUT_ALIASES,
        source_row=source_row,
        raw_payload={"raw": dict(raw)},
        reject_unknown_input_fields=True,
    )
    if isinstance(adapted, RejectRow):
        return adapted

    # specific normalizations for `retail_transactions.csv` pre-parsing
    canon = canonicalize_retail_transaction_row(adapted)

    # ensure only expected keys to get parsed actually get parsed 
    # (`None` caught by parser if invalid for that field)
    canon = {k: canon.get(k) for k in _RETAIL_KNOWN}

    return retail_transactions_parser.parse(
        canon,
        source_row=source_row,
        raw_payload={"raw": dict(raw), "canonical": canon},
    )
