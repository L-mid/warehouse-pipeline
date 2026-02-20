from __future__ import annotations

from typing import Any, Mapping

from warehouse_pipeline.parsing.adapter import adapt_row
from warehouse_pipeline.parsing.primitives import parse_date_yyyy_mm_dd, parse_int, parse_optional_text, parse_required_text
from warehouse_pipeline.parsing.schema import FieldSpec, RowParser
from warehouse_pipeline.parsing.types import RejectRow


def _derive_full_name(canon: dict[str, Any]) -> dict[str, Any]:
    """Derives `full_name` if is `None`."""
    if canon.get("full_name") is None:
        first = canon.pop("first_name", None)
        last = canon.pop("last_name", None)
        if first is not None or last is not None:
            full = f"{(str(first or '').strip())} {(str(last or '').strip())}".strip()
            canon["full_name"] = full if full else None
    else:
        canon.pop("first_name", None)
        canon.pop("last_name", None)
    return canon


def canonicalize_customer_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonicalization/derivation step for customer-shaped data.
    `customers-1000.csv` assumes canonical keys already.
    """
    full_name = _derive_full_name(
        full_name=raw.get("full_name"),
        first_name=raw.get("first_name"),
        last_name=raw.get("last_name"),
    )

    return {
        "customer_id": raw.get("customer_id"),
        "full_name": full_name,
        "email": raw.get("email"),
        "signup_date": raw.get("signup_date"),
        "country": raw.get("country"),
    }



# Allowed input keys, how they map to canonical keys
_CUSTOMERS_INPUT_ALIASES: dict[str, str] = {
    # canonical
    "customer_id": "customer_id",
    "full_name": "full_name",
    "email": "email",
    "signup_date": "signup_date",
    "country": "country",
    # CSV header variants specific to `customers-1000.csv`
    "Customer Id": "customer_id",
    "Email": "email",
    "Country": "country",
    "Subscription Date": "signup_date",
    "First Name": "first_name",
    "Last Name": "last_name",
}

# all known expected fields for `customers-1000.csv`
_CUSTOMERS_KNOWN = {"customer_id", "full_name", "email", "signup_date", "country"}

customers_parser = RowParser(
    known_fields=_CUSTOMERS_KNOWN,
    reject_unknown_fields=True,     # catches bugs where canonicalization introduces extra keys
    fields=[
        FieldSpec(
            out_name="customer_id",
            getter=lambda r: r.get("customer_id"),
            parser=lambda v: parse_int(v, field="customer_id"),
            required=True,
        ),
        FieldSpec(
            out_name="full_name",
            getter=lambda r: r.get("full_name"),
            parser=lambda v: parse_required_text(v, field="full_name"),
            required=True,
        ),
        FieldSpec(
            out_name="email",
            getter=lambda r: r.get("email"),
            parser=parse_optional_text,
            required=False,
        ),
        FieldSpec(
            out_name="signup_date",
            getter=lambda r: r.get("signup_date"),
            parser=lambda v: parse_date_yyyy_mm_dd(v, field="signup_date"),
            required=True,
        ),
        FieldSpec(
            out_name="country",
            getter=lambda r: r.get("country"),
            parser=parse_optional_text,
            required=False,
        ),
    ],
)
 
def parse_customer_row(raw: Mapping[str, Any], *, source_row: int) -> object:
    """Parse a single customer's row."""
    adapted = adapt_row(
        raw,
        aliases=_CUSTOMERS_INPUT_ALIASES,
        source_row=source_row,
        raw_payload={"raw": dict(raw)},
        reject_unknown_input_fields=True,
    )    
    if isinstance(adapted, RejectRow):
        return adapted
    
    canon = _derive_full_name(adapted)      # specific normalizations for `customers-1000.csv` pre-parsing

    # ensure only expected keys to get parsed actually get parsed 
    # (`None` caught by parser if invalid for that field)
    canon = {k: canon.get(k) for k in _CUSTOMERS_KNOWN}

    return customers_parser.parse(canon, source_row=source_row, raw_payload={"raw": dict(raw), "canonical": canon})
