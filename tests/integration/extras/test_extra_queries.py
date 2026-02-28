from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.warehouse.build import build_warehouse


def _run_query_file(conn, path: Path):
    """Simple query reader."""
    sql = path.read_text(encoding="utf-8")
    return conn.execute(sql).fetchall()


@pytest.fixture()
def _built_warehouse(conn, repo_root):
    """
    Load golden inputs (should be 100% clean with no rejects, no dupes, no error),
    then build warehouse dims/facts on that data conveniently for these tests.
    """
    s_customers = load_file(
        conn,
        input_path=repo_root / "data/sample/golden/customers_golden.csv",
        table_name="stg_customers",
    )
    s_orders = load_file(
        conn,
        input_path=repo_root / "data/sample/golden/orders_golden.csv",
        table_name="stg_orders",
    )
    s_items = load_file(
        conn,
        input_path=repo_root / "data/sample/golden/order_items_golden.csv",
        table_name="stg_order_items",
    )

    # fail loud if any row rejects or dedupe rejects or anything other than valid parsing happened.
    assert (s_customers.total, s_customers.loaded, s_customers.rejected) == (4, 4, 0)
    assert (s_orders.total, s_orders.loaded, s_orders.rejected) == (9, 9, 0)
    assert (s_items.total, s_items.loaded, s_items.rejected) == (9, 9, 0)

    build_warehouse(conn)


def test_extra_revenue_by_day_country_golden(conn, repo_root, _built_warehouse):
    """
    Expected from `orders_golden.csv`:

    paid orders:
      2026-02-01 GB 39.97 (1001)
      2026-02-01 US 19.98 (1002)
      2026-02-04 GB 120.00 (1005)
      2026-02-05 GB 15.00 (1006)
      2026-02-06 US 9.99 (1007)
      2026-02-07 CA 75.50 (1008)
    """
    rows = _run_query_file(conn, repo_root / "sql/extras/010_revenue_by_day_country.sql")

    assert rows == [
        (date(2026, 2, 1), "GB", Decimal("39.97"), 1),
        (date(2026, 2, 1), "US", Decimal("19.98"), 1),
        (date(2026, 2, 4), "GB", Decimal("120.00"), 1),
        (date(2026, 2, 5), "GB", Decimal("15.00"), 1),
        (date(2026, 2, 6), "US", Decimal("9.99"), 1),
        (date(2026, 2, 7), "CA", Decimal("75.50"), 1),
    ]



def test_extras_paid_vs_refunded_counts_golden(conn, repo_root, _built_warehouse):
    """
    Expected from `orders_golden.csv` (because all orders count toward `total_orders`):

      CA: paid=1 (1008), refunded=1 (1003), total=2
      GB: paid=3 (1001,1005,1006), refunded=1 (1009), total=4
      US: paid=2 (1002,1007), refunded=0, total=3 (includes canceled 1004)
    """
    rows = _run_query_file(conn, repo_root / "sql/extras/030_paid_vs_refunded_counts.sql")

    assert rows == [
        ("CA", 1, 1, 2),
        ("GB", 3, 1, 4),
        ("US", 2, 0, 3),
    ]