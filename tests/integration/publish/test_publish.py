from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from tests.integration.helpers.square import (
    build_publish_run,
    square_line,
    square_order,
    square_tender,
)
from warehouse_pipeline.publish.views import run_metric_query


def _publish_orders() -> list[dict]:
    return [
        square_order(
            order_id="ord-100",
            state="COMPLETED",
            created_at="2026-03-10T10:00:00Z",
            updated_at="2026-03-10T10:05:00Z",
            closed_at="2026-03-10T10:05:00Z",
            total_money_cents=2300,
            net_total_money_cents=2000,
            total_discount_cents=200,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-espresso",
                    catalog_object_id="item-espresso",
                    name="Espresso",
                    quantity="1",
                    base_price_cents=1200,
                    gross_sales_cents=1200,
                    net_sales_cents=1200,
                ),
                square_line(
                    uid="line-cookie",
                    catalog_object_id="item-cookie",
                    name="Cookie",
                    quantity="2",
                    base_price_cents=400,
                    gross_sales_cents=800,
                    net_sales_cents=800,
                ),
            ],
            tenders=[
                square_tender(
                    tender_id="tender-100",
                    tender_type="CARD",
                    amount_cents=2300,
                    card_brand="VISA",
                )
            ],
        ),
        square_order(
            order_id="ord-101",
            state="COMPLETED",
            created_at="2026-03-10T11:00:00Z",
            updated_at="2026-03-10T11:05:00Z",
            closed_at="2026-03-10T11:05:00Z",
            total_money_cents=1100,
            net_total_money_cents=1000,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-bagel-completed",
                    catalog_object_id="item-bagel",
                    name="Bagel",
                    quantity="1",
                    base_price_cents=1000,
                    gross_sales_cents=1000,
                    net_sales_cents=1000,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-101",
                    tender_type="CASH",
                    amount_cents=1100,
                )
            ],
        ),
        square_order(
            order_id="ord-102",
            state="CANCELED",
            created_at="2026-03-11T09:00:00Z",
            updated_at="2026-03-11T09:05:00Z",
            closed_at="2026-03-11T09:05:00Z",
            total_money_cents=1500,
            net_total_money_cents=1500,
            line_items=[
                square_line(
                    uid="line-bagel-cancelled",
                    catalog_object_id="item-bagel",
                    name="Bagel",
                    quantity="1",
                    base_price_cents=1500,
                    gross_sales_cents=1500,
                    net_sales_cents=1500,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-102",
                    tender_type="CARD",
                    amount_cents=1500,
                    card_brand="VISA",
                )
            ],
        ),
    ]


@pytest.mark.docker_required
def test_publish_views_expose_all_five_flagship_metrics(conn) -> None:
    _, _, _, build_result, publish_result = build_publish_run(conn, orders=_publish_orders())

    assert build_result.files_ran == (
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    )
    assert publish_result.files_ran == ("900_views.sql",)
    assert publish_result.metrics_available == (
        "010_daily_sales_summary",
        "020_daily_sales_by_tender_type",
        "030_weekly_top_items",
        "040_daily_discount_summary",
        "050_daily_order_state_summary",
    )


@pytest.mark.docker_required
def test_publish_metric_queries_return_expected_values_on_smoke_data(conn) -> None:
    build_publish_run(conn, orders=_publish_orders())

    metric_010 = run_metric_query(conn, name="010_daily_sales_summary")
    assert metric_010.columns == (
        "business_date",
        "completed_order_count",
        "total_money",
        "net_total_money",
        "total_discount_money",
        "total_tax_money",
        "total_tip_money",
    )
    assert metric_010.rows == (
        {
            "business_date": date(2026, 3, 10),
            "completed_order_count": 2,
            "total_money": Decimal("34.00"),
            "net_total_money": Decimal("30.00"),
            "total_discount_money": Decimal("2.00"),
            "total_tax_money": Decimal("2.00"),
            "total_tip_money": Decimal("0.00"),
        },
        {
            "business_date": date(2026, 3, 11),
            "completed_order_count": 0,
            "total_money": Decimal("0.00"),
            "net_total_money": Decimal("0.00"),
            "total_discount_money": Decimal("0.00"),
            "total_tax_money": Decimal("0.00"),
            "total_tip_money": Decimal("0.00"),
        },
    )

    metric_020 = run_metric_query(conn, name="020_daily_sales_by_tender_type")
    assert metric_020.rows == (
        {
            "business_date": date(2026, 3, 10),
            "tender_type": "CARD",
            "tender_count": 1,
            "tender_amount_money": Decimal("23.00"),
            "tender_tip_money": Decimal("0.00"),
        },
        {
            "business_date": date(2026, 3, 10),
            "tender_type": "CASH",
            "tender_count": 1,
            "tender_amount_money": Decimal("11.00"),
            "tender_tip_money": Decimal("0.00"),
        },
        {
            "business_date": date(2026, 3, 11),
            "tender_type": "CARD",
            "tender_count": 0,
            "tender_amount_money": Decimal("0.00"),
            "tender_tip_money": Decimal("0.00"),
        },
    )

    metric_030 = run_metric_query(conn, name="030_weekly_top_items")
    assert metric_030.rows == (
        {
            "week_start": date(2026, 3, 9),
            "item_rank": 1,
            "item_key": "item-espresso",
            "item_name": "Espresso",
            "variation_name": None,
            "quantity_sold": Decimal("1.000"),
            "net_sales_money": Decimal("12.00"),
        },
        {
            "week_start": date(2026, 3, 9),
            "item_rank": 2,
            "item_key": "item-bagel",
            "item_name": "Bagel",
            "variation_name": None,
            "quantity_sold": Decimal("1.000"),
            "net_sales_money": Decimal("10.00"),
        },
        {
            "week_start": date(2026, 3, 9),
            "item_rank": 3,
            "item_key": "item-cookie",
            "item_name": "Cookie",
            "variation_name": None,
            "quantity_sold": Decimal("2.000"),
            "net_sales_money": Decimal("8.00"),
        },
    )

    metric_040 = run_metric_query(conn, name="040_daily_discount_summary")
    assert metric_040.rows == (
        {
            "business_date": date(2026, 3, 10),
            "completed_order_count": 2,
            "total_discount_money": Decimal("2.00"),
            "total_money": Decimal("34.00"),
            "discount_rate": Decimal("0.0588"),
        },
        {
            "business_date": date(2026, 3, 11),
            "completed_order_count": 0,
            "total_discount_money": Decimal("0.00"),
            "total_money": Decimal("0.00"),
            "discount_rate": Decimal("0.0000"),
        },
    )

    metric_050 = run_metric_query(conn, name="050_daily_order_state_summary")
    assert metric_050.rows == (
        {
            "business_date": date(2026, 3, 10),
            "order_state": "COMPLETED",
            "order_count": 2,
            "total_money": Decimal("34.00"),
            "net_total_money": Decimal("30.00"),
        },
        {
            "business_date": date(2026, 3, 11),
            "order_state": "CANCELED",
            "order_count": 1,
            "total_money": Decimal("15.00"),
            "net_total_money": Decimal("15.00"),
        },
    )
