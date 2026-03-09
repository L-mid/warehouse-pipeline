from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.stage import (
    MappedCarts,
    MappedProducts,
    MappedUsers,
    ProductLookupItem,
    StageReject,
    StageRow,
    StageTableLoadResult,
    UserLookupItem,
)


def test_stage_init_happy_path() -> None:
    """Stage package exports the expected dataclasses and defaults."""
    row = StageRow(
        table_name="stg_products",
        source_ref=1,
        raw_payload={"id": 10},
        values={"product_id": 10, "price_usd": Decimal("9.99")},
    )

    reject = StageReject(
        table_name="stg_orders",
        source_ref=2,
        raw_payload={"id": 100},
        reason_code="bad_row",
        reason_detail="example",
    ) 

    product = ProductLookupItem(
        product_id=10,
        sku="SKU-groceries-tea-10",
        title="Tea",
        category="groceries",
        unit_price_usd=Decimal("4.99"),
        discount_pct=None,
    )

    user = UserLookupItem(
        customer_id=1,
        country="UK",
        city="London",
        email="ada@example.com",
    )
    result = StageTableLoadResult(
        table_name="stg_products",
        inserted_count=1,
        duplicate_reject_count=0,
        explicit_reject_count=0,
    )

    mapped_users = MappedUsers(user_lookup={1: user})
    mapped_products = MappedProducts(product_lookup={10: product})
    mapped_carts = MappedCarts()          


    assert row.table_name == "stg_products"
    assert reject.reason_code == "bad_row"
    assert product.sku == "SKU-groceries-tea-10"
    assert user.email == "ada@example.com"
    assert result.inserted_count == 1

    assert mapped_users.rows == []
    assert mapped_products.rows == []
    assert mapped_carts.order_rows == []   




