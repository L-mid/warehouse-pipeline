from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


_Q2 = Decimal("0.01")
_Q4 = Decimal("0.0001")
_BASE_ORDER_TS = datetime(2024, 1, 1, tzinfo=UTC)


def normalize_text(value: str | None) -> str | None:
    """Strip text and collapse blank strings to `None`."""
    if value is None:
        return None
    value = value.strip()
    return value or None

def normalize_email(value: str | None) -> str | None:
    """Normalize email casing while preserving `None` for missing values."""
    value = normalize_text(value)
    if value is None:
        return None
    return value.lower()

def to_decimal(value: Any) -> Decimal | None:
    """Convert numeric-ish input to `Decimal` via `str()`."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def quantize_money(value: Any) -> Decimal | None:
    """Quantize money values to numeric(12,2)-like precision."""
    dec = to_decimal(value)
    if dec is None:
        return None
    return dec.quantize(_Q2, rounding=ROUND_HALF_UP)

def quantize_pct(value: Any) -> Decimal | None:
    """Quantize percentage values to numeric(8,4)-like precision."""
    dec = to_decimal(value)
    if dec is None:
        return None
    return dec.quantize(_Q4, rounding=ROUND_HALF_UP)

def derive_full_name(first_name: str | None, last_name: str | None) -> str | None:
    """Build a full name from first/last name components."""
    first = normalize_text(first_name)
    last = normalize_text(last_name)
    if first and last:
        return f"{first} {last}"
    return first or last


def slugify(value: str | None) -> str:
    """Create a stable slug for derived identifiers such as `SKU`."""
    text = normalize_text(value)
    if text is None:
        return "unknown"
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"

def derive_sku(*, product_id: int, category: str | None, title: str | None) -> str:
    """Build a deterministic SKU from stable product attributes."""
    return f"SKU-{slugify(category)}-{slugify(title)}-{product_id}"


def derive_order_status(*, cart_id: int, total_products: int, total_quantity: int) -> str:
    """
    Derive a deterministic synthetic status.

    DummyJSON mappings:

    - empty carts become "canceled"
    - a deterministic slice become "refunded"
    - a deterministic slice become "pending"
    - everything else becomes "paid"
    """
    if total_products == 0 or total_quantity == 0:
        return "canceled"

    bucket = cart_id % 20   # slice
    if bucket == 0:
        return "refunded"
    if bucket in (1, 2, 3):
        return "pending"
    return "paid"


def derive_order_ts(*, cart_id: int, user_id: int) -> datetime:
    """
    Derive a deterministic timestamp when the source lacks a real one.
    Stable across reruns of the same snapshot 
    """
    day_offset = cart_id % 365
    minute_offset = ((user_id * 17) + cart_id) % (24 * 60)
    second_offset = cart_id % 60
    return _BASE_ORDER_TS + timedelta(
        days=day_offset,
        minutes=minute_offset,
        seconds=second_offset,
    )


def derive_line_discount_pct(*, line_total: Any, discounted_line_total: Any) -> Decimal:
    """Compute line discount percent from pre/post-discount line totals."""
    total = to_decimal(line_total) or Decimal("0")
    discounted = to_decimal(discounted_line_total)

    if total <= 0 or discounted is None:
        return Decimal("0.0000")

    discount = Decimal("1") - (discounted / total)
    if discount < 0:
        discount = Decimal("0")
    if discount > 1:
        discount = Decimal("1")
    return quantize_pct(discount) or Decimal("0.0000")


def derive_gross_usd(*, quantity: int, unit_price_usd: Any) -> Decimal:
    """Compute gross line value from quantity and unit price."""
    unit_price = to_decimal(unit_price_usd) or Decimal("0")
    gross = Decimal(quantity) * unit_price
    return quantize_money(gross) or Decimal("0.00")


def derive_net_usd(
    *,
    gross_usd: Any,
    discount_pct: Any,
    discounted_line_total: Any,
) -> Decimal:
    """
    Compute net line value.

    Prefer the explicit discounted line total when present, otherwise fall back
    to `gross * (1 - discount_pct)`.
    """
    explicit_total = quantize_money(discounted_line_total)
    if explicit_total is not None:
        return explicit_total

    gross = to_decimal(gross_usd) or Decimal("0")
    pct = to_decimal(discount_pct) or Decimal("0")
    net = gross * (Decimal("1") - pct)
    return quantize_money(net) or Decimal("0.00")