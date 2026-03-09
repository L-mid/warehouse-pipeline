from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.extract.models import DummyProduct
from warehouse_pipeline.stage.map_products import map_products



def test_map_products_happy_path() -> None:
    """Products maps into `stg_products` rows and a product lookup."""
    products = [
        DummyProduct(
            id=10,
            title="Tea",
            category="groceries",
            price=4.99,
            stock=12,
        )
    ]

    mapped = map_products(products)

    assert len(mapped.rows) == 1
    assert mapped.rejects == []
    assert 10 in mapped.product_lookup

    row = mapped.rows[0]
    lookup = mapped.product_lookup[10]

    assert row.table_name == "stg_products"
    assert row.values["product_id"] == 10
    assert row.values["sku"] == "SKU-groceries-tea-10"
    assert row.values["price_usd"] == Decimal("4.99")
    assert row.values["stock"] == 12

    assert lookup.sku == "SKU-groceries-tea-10"
    assert lookup.unit_price_usd == Decimal("4.99")   