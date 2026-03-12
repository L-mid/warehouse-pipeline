from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.extract.models import DummyCart, DummyCartProduct
from warehouse_pipeline.stage import ProductLookupItem, UserLookupItem
from warehouse_pipeline.stage.map_carts import map_carts


def test_map_carts_happy_path() -> None:
    """A cart maps into one `stg_orders` row and one `stg_order_items row`."""
    carts = [
        DummyCart(
            id=100,
            userId=1,
            total=9.98,
            discountedTotal=7.48,
            totalProducts=1,
            totalQuantity=2,
            products=[
                DummyCartProduct(
                    id=10,
                    quantity=2,
                    price=4.99,
                    total=9.98,
                    discountedTotal=7.48,
                )
            ],
        )
    ]

    product_lookup = {
        10: ProductLookupItem(
            product_id=10,
            sku="SKU-groceries-tea-10",
            title="Tea",
            category="groceries",
            unit_price_usd=Decimal("4.99"),
            discount_pct=None,
        )
    }
    user_lookup = {
        1: UserLookupItem(
            customer_id=1,
            country="UK",
            city="London",
            email="ada@example.com",
        )
    }

    mapped = map_carts(carts, product_lookup=product_lookup, user_lookup=user_lookup)

    assert len(mapped.order_rows) == 1
    assert len(mapped.order_item_rows) == 1
    assert mapped.rejects == []

    order = mapped.order_rows[0]
    item = mapped.order_item_rows[0]

    assert order.table_name == "stg_orders"
    assert order.values["order_id"] == 100
    assert order.values["customer_id"] == 1
    assert order.values["country"] == "UK"
    assert order.values["total_usd"] == Decimal("7.48")

    assert item.table_name == "stg_order_items"
    assert item.values["order_id"] == 100
    assert item.values["line_id"] == 1
    assert item.values["product_id"] == 10
    assert item.values["sku"] == "SKU-groceries-tea-10"
    assert item.values["gross_usd"] == Decimal("9.98")
    assert item.values["net_usd"] == Decimal("7.48")
