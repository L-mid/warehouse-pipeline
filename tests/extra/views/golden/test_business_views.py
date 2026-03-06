from __future__ import annotations

from warehouse_pipeline.cli.loader import load_file

from datetime import date
from decimal import Decimal




def test_business_views_daily_revenue_and_new_customers_by_day(conn, repo_root) -> None:
    """
    Lighter integration tests on golden `.csv` files for asserting business views.
    """
    # Loads golden fixtures 
    # each load produces a `run_id`. 
    # on success, marks it succeeded
    load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "golden" / "customers_golden.csv",
        table_name="stg_customers",
    )
    load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "golden" / "retail_transactions_golden.csv",
        table_name="stg_retail_transactions",
    )


    # `new_customers_by_day`
    cust_rows = conn.execute(
        "SELECT day, new_customers FROM new_customers_by_day ORDER BY day ASC"
    ).fetchall()

    # exact expectations
    assert cust_rows == [
        (date(2026, 1, 1), 2),
        (date(2026, 1, 2), 1),
        (date(2026, 1, 4), 1),
    ]


    # `daily_revenue` (special paid rule is: `units_sold > 0 AND revenue > 0`)
    rev_rows = conn.execute(
        "SELECT day, paid_revenue_usd, paid_units_sold, paid_rows FROM daily_revenue ORDER BY day ASC"
    ).fetchall()

    # exact expectations
    assert rev_rows == [
        (date(2026, 1, 1), Decimal("10.00"), 2, 1),
        (date(2026, 1, 2), Decimal("20.00"), 4, 2),
    ]


