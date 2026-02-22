from __future__ import annotations

from pathlib import Path

import pytest
# import pdb; pdb.set_trace()       # create breakpoints

from warehouse_pipeline.cli.loader import load_file

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
    # clean slate (schema already exists)
    conn.execute(
        "TRUNCATE reject_rows, stg_retail_transactions, stg_customers, ingest_runs RESTART IDENTITY CASCADE;"
    )

    # run loads
    customers_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "customers-1000.csv",
        table_name="stg_customers",
    )
    retail_transactions_summary = load_file(
        conn,
        input_path=repo_root / "data" / "sample" / "retail_transactions.csv",
        table_name="stg_retail_transactions",
    )

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


    assert customers_summary.total == customers_summary.loaded + customers_summary.rejected
    assert stg_customers_ct == customers_summary.loaded
    assert reject_customers_ct == customers_summary.rejected
    assert stg_customers_ct > 0

    ## -- retail_transactions assertions 
    stg_retail_transactions_ct = conn.execute(
        "SELECT COUNT(*) FROM stg_retail_transactions WHERE run_id = %s",
        (retail_transactions_summary.run_id,),
    ).fetchone()[0]
    reject_retail_transactions_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_retail_transactions'",
        (retail_transactions_summary.run_id,),
    ).fetchone()[0]

    assert retail_transactions_summary.total == retail_transactions_summary.loaded + retail_transactions_summary.rejected
    assert stg_retail_transactions_ct == retail_transactions_summary.loaded
    assert reject_retail_transactions_ct == retail_transactions_summary.rejected
    assert stg_retail_transactions_ct > 0