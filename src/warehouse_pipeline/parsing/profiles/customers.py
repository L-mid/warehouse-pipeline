from __future__ import annotations

from warehouse_pipeline.parsing.adapter import adapt_row
from warehouse_pipeline.parsing.primitives import (
    parse_date_yyyy_mm_dd, 
    parse_optional_text, 
    parse_required_text
)
from warehouse_pipeline.parsing.schema import FieldSpec, RowParser
from warehouse_pipeline.parsing.types import RejectRow

from typing import Any, Mapping
from dataclasses import dataclass


def _derive_full_name(canon: dict[str, Any]) -> dict[str, Any]:
    """Derives `full_name` if missing/`None`/empty-ish, using `first_name` + `last_name`."""
    full = canon.get("full_name")
    if full is None or str(full).strip() == "":
        first = canon.get("first_name")
        last = canon.get("last_name")
        derived = f"{(str(first or '').strip())} {(str(last or '').strip())}".strip()
        canon["full_name"] = derived if derived else None
    return canon


def canonicalize_customer_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonicalization/derivation step for customer-shaped data.
    - `full_name` field is derived from first and last name if not provied.
    
    Input is expected to already be transformed into canonical keys via `adapt_row` aliases.
    Output keys should align with `_CUSTOMERS_KNOWN`.
    """
    canon: dict[str, Any] = dict(raw)

    canon = _derive_full_name(canon)        # add the derived field `full_name`

    return {
        "customer_id": canon.get("customer_id"),
        "first_name": canon.get("first_name"),
        "last_name": canon.get("last_name"),
        "full_name": canon.get("full_name"),
        "company": canon.get("company"),
        "city": canon.get("city"),
        "country": canon.get("country"),
        "phone_1": canon.get("phone_1"),
        "phone_2": canon.get("phone_2"),
        "email": canon.get("email"),
        "subscription_date": canon.get("subscription_date"),
        "website": canon.get("website"),
    }


 
# Allowed input keys, how they map to canonical keys
_CUSTOMERS_INPUT_ALIASES: dict[str, str] = {
    # canonical
    "customer_id": "customer_id",
    "first_name": "first_name",
    "last_name": "last_name",
    "full_name": "full_name",
    "company": "company",
    "city": "city",
    "country": "country",
    "phone_1": "phone_1",
    "phone_2": "phone_2",
    "email": "email",
    "subscription_date": "subscription_date",
    "website": "website",
    # CSV header variants specific to `customers-1000.csv`
    "Index": "source_index",                 # accept this header now but later drop
    "Customer Id": "customer_id",
    "First Name": "first_name",
    "Last Name": "last_name",
    "Company": "company",
    "City": "city",
    "Country": "country",
    "Phone 1": "phone_1",
    "Phone 2": "phone_2",
    "Email": "email",
    "Subscription Date": "subscription_date",
    "Website": "website",
}

# all known expected fields for `customers-1000.csv`
_CUSTOMERS_KNOWN = set(_CUSTOMERS_INPUT_ALIASES.values())


customers_parser = RowParser(
    known_fields=_CUSTOMERS_KNOWN,
    reject_unknown_fields=True,     # catches bugs where canonicalization introduces extra keys
    fields=[
        FieldSpec("customer_id", lambda r: r.get("customer_id"), lambda v: parse_required_text(v, field="customer_id"), True),

        FieldSpec("first_name", lambda r: r.get("first_name"), lambda v: parse_required_text(v, field="first_name"), True),
        FieldSpec("last_name", lambda r: r.get("last_name"), lambda v: parse_required_text(v, field="last_name"), True),
        FieldSpec("full_name", lambda r: r.get("full_name"), lambda v: parse_required_text(v, field="full_name"), True),

        FieldSpec("company", lambda r: r.get("company"), parse_optional_text, False),
        FieldSpec("city", lambda r: r.get("city"), parse_optional_text, False),
        FieldSpec("country", lambda r: r.get("country"), parse_optional_text, False),

        FieldSpec("phone_1", lambda r: r.get("phone_1"), parse_optional_text, False),
        FieldSpec("phone_2", lambda r: r.get("phone_2"), parse_optional_text, False),

        FieldSpec("email", lambda r: r.get("email"), parse_optional_text, False),

        FieldSpec("subscription_date", lambda r: r.get("subscription_date"), lambda v: parse_date_yyyy_mm_dd(v, field="subscription_date"), True),

        FieldSpec("website", lambda r: r.get("website"), parse_optional_text, False),
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


@dataclass(frozen=True)
class FunctionRowParser:
    """Return `parse_customer_row` in an object, for imports."""
    fn: Any  # must be callable

    def parse(self, raw: Mapping[str, Any], *, source_row: int) -> object:
        """Return the provided parser's outputs."""
        return self.fn(raw, source_row=source_row)

# import
CUSTOMER_PARSER = FunctionRowParser(parse_customer_row)
