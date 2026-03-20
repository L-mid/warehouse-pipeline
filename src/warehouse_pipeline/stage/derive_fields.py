from __future__ import annotations

import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

_Q2 = Decimal("0.01")
_Q4 = Decimal("0.0001")


def normalize_text(value: str | None) -> str | None:
    """Strip text and collapse blank strings to `None`."""
    if value is None:
        return None
    value = value.strip()
    return value or None


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


def slugify(value: str | None) -> str:
    """Create a stable slug for derived identifiers such as `SKU`."""
    text = normalize_text(value)
    if text is None:
        return "unknown"
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


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
