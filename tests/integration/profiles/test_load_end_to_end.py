from __future__ import annotations

from pathlib import Path

import pytest
# import pdb; pdb.set_trace()       # create breakpoints

from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.dq.runner import run_dq

# NOTE:
## -- the schema is created by docker-entrypoint-initdb.d (./sql mount) on a fresh volume

def debug_reject_breakdown(conn, run_id, table_name: str) -> None:
    """
    Displays reject rows. 
    Call after run is complete in the test to print 10 lines of `reject_rows`
    in the terminal.
    """
    rows = conn.execute(
        """
        SELECT reason_code, COUNT(*) AS n, MIN(reason_detail) AS example
        FROM reject_rows
        WHERE run_id = %s AND table_name = %s       
        GROUP BY 1
        ORDER BY n DESC, reason_code ASC
        """,
        (run_id, table_name),
    ).fetchall()
    print(f"[DEBUG] reject breakdown for {table_name} {run_id}:")
    for r in rows[:10]:
        print(" ", r)


# @pytest.mark.integration      add these marks
def test_load_end_to_end_customers_and_retail_transactions(conn, repo_root) -> None:
    """
    Heavy end-to-end test using full `.csv` sample data.
    """
    # clean slate testing ground (because schema already exists)
    conn.execute(
        "TRUNCATE reject_rows, stg_retail_transactions, stg_customers, ingest_runs RESTART IDENTITY CASCADE;"
    )

    # customers
    customers_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "customers-1000.csv",
        table_name="stg_customers",
    )
    run_dq(conn, run_id=customers_summary.run_id, table_name=customers_summary.table_name)

    # retail_transactions
    retail_transactions_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "retail_transactions.csv",
        table_name="stg_retail_transactions",
    )
    run_dq(conn, run_id=retail_transactions_summary.run_id, table_name=retail_transactions_summary.table_name)

    # constantly useful to call this if debugging:
    """
    print("[DEBUG]")
    print("customers_summary:", customers_summary)
    print("retail_transactions_summary:", retail_transactions_summary)

    debug_reject_breakdown(
        conn, 
        run_id=customers_summary.run_id, 
        table_name=customers_summary.table_name,
    )
    """


    ## -- customers assertions 
    stg_customers_ct = conn.execute(
        "SELECT COUNT(*) FROM stg_customers WHERE run_id = %s",
        (customers_summary.run_id,),
    ).fetchone()[0]
    reject_customers_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_customers'",
        (customers_summary.run_id,),
    ).fetchone()[0]
    dq_customers_ct = conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s AND table_name = %s",
        (customers_summary.run_id, customers_summary.table_name),
    ).fetchone()[0]


    assert customers_summary.total == customers_summary.loaded + customers_summary.rejected
    assert stg_customers_ct == customers_summary.loaded
    assert reject_customers_ct == customers_summary.rejected
    assert stg_customers_ct > 0
    assert dq_customers_ct > 0

    ## -- retail_transactions assertions 
    stg_retail_transactions_ct = conn.execute(
        "SELECT COUNT(*) FROM stg_retail_transactions WHERE run_id = %s",
        (retail_transactions_summary.run_id,),
    ).fetchone()[0]
    reject_retail_transactions_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_retail_transactions'",
        (retail_transactions_summary.run_id,),
    ).fetchone()[0]
    dq_retail_ct = conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s AND table_name = %s",
        (retail_transactions_summary.run_id, retail_transactions_summary.table_name),
    ).fetchone()[0]

    assert retail_transactions_summary.total == retail_transactions_summary.loaded + retail_transactions_summary.rejected
    assert stg_retail_transactions_ct == retail_transactions_summary.loaded
    assert reject_retail_transactions_ct == retail_transactions_summary.rejected
    assert stg_retail_transactions_ct > 0
    assert dq_retail_ct > 0




def test_load_end_to_end_orders_and_order_items(conn, repo_root) -> None:
    """Heavy end-to-end test using full `.csv` sample data, from `orders.csv` and `order_items.csv`."""
    # clean slate testing ground (because schema already exists)
    conn.execute(
        "TRUNCATE reject_rows, dq_results, stg_order_items, stg_orders, ingest_runs RESTART IDENTITY CASCADE;"
    )

    # orders
    orders_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "orders.csv",
        table_name="stg_orders",
    )
    run_dq(conn, run_id=orders_summary.run_id, table_name=orders_summary.table_name)

    # order_items
    items_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "order_items.csv",
        table_name="stg_order_items",
    )
    run_dq(conn, run_id=items_summary.run_id, table_name=items_summary.table_name)



    # rows landed in stg_*
    stg_orders_ct = conn.execute(
        "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
        (orders_summary.run_id,),
    ).fetchone()[0]
    stg_items_ct = conn.execute(
        "SELECT COUNT(*) FROM stg_order_items WHERE run_id = %s",
        (items_summary.run_id,),
    ).fetchone()[0]

    # rejects landed in reject_rows
    rej_orders_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_orders'",
        (orders_summary.run_id,),
    ).fetchone()[0]
    rej_items_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_order_items'",
        (items_summary.run_id,),
    ).fetchone()[0]

    # ingest_runs has both runs
    runs_ct = conn.execute(
        "SELECT COUNT(*) FROM ingest_runs WHERE run_id IN (%s, %s)",
        (orders_summary.run_id, items_summary.run_id),
    ).fetchone()[0]

    # dq results exist for both
    dq_orders_ct = conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s AND table_name = %s",
        (orders_summary.run_id, orders_summary.table_name),
    ).fetchone()[0]
    dq_items_ct = conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s AND table_name = %s",
        (items_summary.run_id, items_summary.table_name),
    ).fetchone()[0]

    assert orders_summary.total == orders_summary.loaded + orders_summary.rejected
    assert items_summary.total == items_summary.loaded + items_summary.rejected

    assert stg_orders_ct == orders_summary.loaded
    assert stg_items_ct == items_summary.loaded

    assert rej_orders_ct == orders_summary.rejected
    assert rej_items_ct == items_summary.rejected

    assert stg_orders_ct > 0
    assert stg_items_ct > 0
    assert runs_ct == 2
    assert dq_orders_ct > 0
    assert dq_items_ct > 0

    # since samples are "messy on purpose", make sure we actually exercised rejects:
    assert (rej_orders_ct + rej_items_ct) > 0