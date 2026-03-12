from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.stage.derive_fields import (
    derive_full_name,
    derive_gross_usd,
    derive_line_discount_pct,
    derive_net_usd,
    derive_order_status,
    derive_order_ts,
    derive_sku,
    normalize_email,
)


# temporary hopefully
def test_derive_fields_happy_path() -> None:
    """The derived helpers produce deterministic and normalized values."""
    assert derive_full_name(" Ada ", " Lovelace ") == "Ada Lovelace"
    assert normalize_email(" ADA@EXAMPLE.COM ") == "ada@example.com"

    sku = derive_sku(product_id=10, category="Groceries", title="Tea Bags")
    assert sku == "SKU-groceries-tea-bags-10"

    ts1 = derive_order_ts(cart_id=100, user_id=1)
    ts2 = derive_order_ts(cart_id=100, user_id=1)
    assert ts1 == ts2
    assert ts1.tzinfo is not None

    assert derive_order_status(cart_id=7, total_products=1, total_quantity=2) == "paid"
    assert derive_order_status(cart_id=40, total_products=1, total_quantity=2) == "refunded"
    assert derive_order_status(cart_id=1, total_products=0, total_quantity=0) == "canceled"

    gross = derive_gross_usd(quantity=2, unit_price_usd="4.99")
    discount_pct = derive_line_discount_pct(line_total="9.98", discounted_line_total="7.48")
    net = derive_net_usd(gross_usd=gross, discount_pct=discount_pct, discounted_line_total="7.48")

    assert gross == Decimal("9.98")
    assert discount_pct == Decimal("0.2505")
    assert net == Decimal("7.48")
