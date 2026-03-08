from __future__ import annotations

from decimal import Decimal

from warehouse_pipeline.db.dq_results import DQMetricRow, upsert_dq_results
from warehouse_pipeline.db.run_ledger import RunStart, create_run, mark_run_succeeded
from warehouse_pipeline.db.work_tables import WorkRow, flush_work_table, insert_work_rows, prepare_work_table
from warehouse_pipeline.db.writers.rejects import RejectInsert, insert_reject_rows




def test_db_happy_path(conn) -> None:
    """db run a bucnh of thign"""

    ## -- creates a run for run ledger.
    run_id = create_run(
        conn,
        entry=RunStart(
            mode="snapshot",
            source_system="dummyjson",
            snapshot_key="dummyjson/v1",
            git_sha="test-sha",
            args_json={"page_size": 1},
        ),
    )

    ## -- typical non dupe rejection test for writing into reject rows.
    inserted_rejects = insert_reject_rows(
        conn,
        run_id=run_id,
        rejects=[
            RejectInsert(
                table_name="stg_products",
                source_ref=999,
                raw_payload={"id": 999},
                reason_code="bad_payload",
                reason_detail="demo reject",
            )
        ],
    )
    assert inserted_rejects == 1

    
    ## -- staging good rows

    # prepare work table
    prepare_work_table(conn, table_name="stg_orders")

    # insert rows (two). 
    # They're dupes of each other, one should be rejected.
    inserted_work = insert_work_rows(
        conn,
        table_name="stg_orders",
        run_id=run_id,
        rows=[
            WorkRow(
                source_ref=1,
                raw_payload={"id": 100},
                values={
                    "order_id": 100,
                    "customer_id": 1,
                    "order_ts": None,
                    "country": "UK",
                    "status": "paid",
                    "total_usd": Decimal("9.99"),
                    "total_products": 1,
                    "total_quantity": 2,
                },
            ),
            WorkRow(
                source_ref=2,
                raw_payload={"id": 100, "dup": True},   # should still flag
                values={
                    "order_id": 100,
                    "customer_id": 1,
                    "order_ts": None,
                    "country": "UK",
                    "status": "paid",
                    "total_usd": Decimal("9.99"),
                    "total_products": 1,
                    "total_quantity": 2,
                },
            ),
        ],
    )
    assert inserted_work == 2   # two rows


    # flush good rows to db and deal with any dupes
    inserted_orders, duplicate_rejects = flush_work_table(
        conn,
        table_name="stg_orders",
        run_id=run_id,
    )

    # the good row
    assert inserted_orders == 1
    # the dupe.
    assert duplicate_rejects == 1


    # data quality has persistence upon upserts.
    inserted_dq = upsert_dq_results(
        conn,
        rows=[
            DQMetricRow(
                run_id=run_id,
                table_name="stg_orders",
                check_name="row_count_positive",
                metric_name="row_count",
                metric_value=Decimal("1"),
                passed=True,
                details_json={"threshold": 1},
            )
        ],
    )
    # one only
    assert inserted_dq == 1

    # status change to "suceeded"
    mark_run_succeeded(conn, run_id=run_id)
   
    assert conn.execute(
        "SELECT status FROM run_ledger WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0] == "succeeded" # worked


    assert conn.execute(
        "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0] == 1    # one row was expected

    assert conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0] == 2    # two rows were expected

    assert conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0] == 1    # one row was expected


