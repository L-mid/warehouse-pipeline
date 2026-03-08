from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import warehouse_pipeline.db.work_tables as work_tables_mod
from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.writers.staging import StagingTableSpec


def test_work_table_happy_path(monkeypatch) -> None:
    """
    A temporary working table is created with good rows, 
    runs dedupe logic, and flushes results to the real staging table.
    """

    conn = FakeConnection(fetchone_rows=[(1, 0)])
    run_id = uuid4()

    # gives it a fake spec
    # used example is `stg_orders`
    spec = StagingTableSpec(
        table_name="stg_orders",
        columns=(
            "order_id",
            "customer_id",
            "order_ts",
            "country",
            "status",
            "total_usd",
            "total_products",
            "total_quantity",
        ),
        key_cols=("order_id",),
    )

    # preparation prepares all tables for tmp before insertion.
    work_tables_mod.prepare_work_table(conn, table_name="stg_orders")

    # insert all good into work rows
    inserted_into_work = work_tables_mod.insert_work_rows(
        conn,
        table_name="stg_orders",
        run_id=run_id,
        rows=[
            # a single row (will not dupe)
            work_tables_mod.WorkRow(
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
            )
        ],
    )
    # sort dupes and good rows, push to real staging
    inserted, duplicates = work_tables_mod.flush_work_table(
        conn,
        table_name="stg_orders",
        run_id=run_id,
    )

    # only one insertion, one session.
    assert inserted_into_work == 1
    assert inserted == 1
    assert duplicates == 0  # duplicates explicitly recorded.




    