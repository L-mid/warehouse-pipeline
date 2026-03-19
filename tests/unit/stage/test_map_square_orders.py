from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.stage.map_square_orders import map_square_orders


def test_map_square_orders_happy_path_builds_order_line_and_tender_rows() -> None:
    orders = [
        {
            "id": "ord-100",
            "location_id": "LOC-1",
            "customer_id": "CUST-1",
            "state": "COMPLETED",
            "created_at": "2026-03-10T10:00:00Z",
            "updated_at": "2026-03-10T10:05:00Z",
            "closed_at": "2026-03-10T10:05:00Z",
            "total_money": {"amount": 2300, "currency": "USD"},
            "total_discount_money": {"amount": 200, "currency": "USD"},
            "total_tax_money": {"amount": 100, "currency": "USD"},
            "total_tip_money": {"amount": 0, "currency": "USD"},
            "net_amounts": {
                "total_money": {"amount": 2000, "currency": "USD"},
                "discount_money": {"amount": 200, "currency": "USD"},
                "tax_money": {"amount": 100, "currency": "USD"},
            },
            "line_items": [
                {
                    "uid": "line-1",
                    "catalog_object_id": "item-espresso",
                    "name": "Espresso",
                    "variation_name": None,
                    "quantity": "2",
                    "base_price_money": {"amount": 1000, "currency": "USD"},
                    "gross_sales_money": {"amount": 2000, "currency": "USD"},
                    "total_discount_money": {"amount": 0, "currency": "USD"},
                    "total_tax_money": {"amount": 0, "currency": "USD"},
                    "total_money": {"amount": 2000, "currency": "USD"},
                }
            ],
            "tenders": [
                {
                    "id": "tender-1",
                    "type": "CARD",
                    "amount_money": {"amount": 2300, "currency": "USD"},
                    "tip_money": {"amount": 0, "currency": "USD"},
                    "card_details": {"card": {"card_brand": "VISA"}},
                }
            ],
        }
    ]

    mapped = map_square_orders(orders)

    assert len(mapped.order_rows) == 1
    assert len(mapped.order_line_rows) == 1
    assert len(mapped.tender_rows) == 1
    assert mapped.rejects == []

    order_row = mapped.order_rows[0]
    assert order_row.table_name == "stg_square_orders"
    assert order_row.values == {
        "order_id": "ord-100",
        "location_id": "LOC-1",
        "customer_id": "CUST-1",
        "state": "COMPLETED",
        "created_at_source": "2026-03-10T10:00:00Z",
        "updated_at_source": "2026-03-10T10:05:00Z",
        "closed_at_source": "2026-03-10T10:05:00Z",
        "currency_code": "USD",
        "total_money": Decimal("23.00"),
        "net_total_money": Decimal("20.00"),
        "total_discount_money": Decimal("2.00"),
        "total_tax_money": Decimal("1.00"),
        "total_tip_money": Decimal("0.00"),
    }

    line_row = mapped.order_line_rows[0]
    assert line_row.table_name == "stg_square_order_lines"
    assert line_row.values == {
        "order_id": "ord-100",
        "line_uid": "line-1",
        "catalog_object_id": "item-espresso",
        "name": "Espresso",
        "variation_name": None,
        "quantity": Decimal("2"),
        "base_price_money": Decimal("10.00"),
        "gross_sales_money": Decimal("20.00"),
        "total_discount_money": Decimal("0.00"),
        "total_tax_money": Decimal("0.00"),
        "net_sales_money": Decimal("20.00"),
        "currency_code": "USD",
    }

    tender_row = mapped.tender_rows[0]
    assert tender_row.table_name == "stg_square_tenders"
    assert tender_row.values == {
        "order_id": "ord-100",
        "tender_id": "tender-1",
        "tender_type": "CARD",
        "card_brand": "VISA",
        "amount_money": Decimal("23.00"),
        "tip_money": Decimal("0.00"),
        "currency_code": "USD",
    }
