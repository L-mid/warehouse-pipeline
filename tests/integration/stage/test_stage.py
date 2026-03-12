from __future__ import annotations

import pytest

from warehouse_pipeline.db.run_ledger import RunStart, create_run
from warehouse_pipeline.extract.models import (
    parse_carts_page,
    parse_products_page,
    parse_users_page,
)
from warehouse_pipeline.stage.load import load_mapped_batches
from warehouse_pipeline.stage.map_carts import map_carts
from warehouse_pipeline.stage.map_products import map_products
from warehouse_pipeline.stage.map_users import map_users


@pytest.mark.docker_required
def test_stage_happy_path(conn) -> None:
    """
    This is the idea
    - Parse source payloads -> map to stage rows -> load into Postgres staging tables.
    """
    users_page = parse_users_page(
        {
            "users": [
                {
                    "id": 1,
                    "firstName": "Ada",
                    "lastName": "Lovelace",
                    "email": "ada@example.com",
                    "phone": "123",
                    "address": {"city": "London", "country": "UK"},
                    "company": {"name": "Analytical Engines Ltd"},
                }
            ],
            "total": 1,
            "skip": 0,
            "limit": 100,
        }
    )

    products_page = parse_products_page(
        {
            "products": [
                {
                    "id": 10,
                    "title": "Tea",
                    "category": "groceries",
                    "price": 4.99,
                    "stock": 12,
                }
            ],
            "total": 1,
            "skip": 0,
            "limit": 100,
        }
    )

    carts_page = parse_carts_page(
        {
            "carts": [
                {
                    "id": 100,
                    "userId": 1,
                    "total": 9.98,
                    "discountedTotal": 7.48,
                    "totalProducts": 1,
                    "totalQuantity": 2,
                    "products": [
                        {
                            "id": 10,
                            "quantity": 2,
                            "price": 4.99,
                            "total": 9.98,
                            "discountedTotal": 7.48,
                        }
                    ],
                }
            ],
            "total": 1,
            "skip": 0,
            "limit": 100,
        }
    )

    mapped_users = map_users(users_page.users)
    mapped_products = map_products(products_page.products)
    mapped_carts = map_carts(
        carts_page.carts,
        product_lookup=mapped_products.product_lookup,
        user_lookup=mapped_users.user_lookup,
    )

    run_id = create_run(conn, entry=RunStart(mode="snapshot", snapshot_key="dummyjson/smoke"))

    results = load_mapped_batches(
        conn,
        run_id=run_id,
        users=mapped_users,
        products=mapped_products,
        carts=mapped_carts,
    )

    assert results["stg_customers"].inserted_count == 1
    assert results["stg_products"].inserted_count == 1
    assert results["stg_orders"].inserted_count == 1
    assert results["stg_order_items"].inserted_count == 1

    customer_count = conn.execute(
        "SELECT COUNT(*) FROM stg_customers WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]
    product_count = conn.execute(
        "SELECT COUNT(*) FROM stg_products WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]
    order_count = conn.execute(
        "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]
    item_count = conn.execute(
        "SELECT COUNT(*) FROM stg_order_items WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]
    reject_count = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]

    # one row expected per
    assert customer_count == 1
    assert product_count == 1
    # including here from carts:
    assert order_count == 1
    assert item_count == 1
    assert reject_count == 0  # all rows were ok
