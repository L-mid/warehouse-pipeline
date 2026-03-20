from __future__ import annotations

from decimal import Decimal
from typing import cast
from uuid import uuid4

import psycopg

import warehouse_pipeline.db.work_tables as work_tables_mod
from tests.unit.db.mocks import FakeConnection


def test_work_table_happy_path() -> None:
    """
    A temporary working table is created for a real staging table,
    rows are inserted into it, and the dedupe flush returns the expected counts.
    """

    fake_conn = FakeConnection(fetchone_rows=[(1, 0)])
    conn = cast(psycopg.Connection[tuple], fake_conn)
    run_id = uuid4()

    work_tables_mod.prepare_work_table(conn, table_name="stg_square_orders")

    inserted_into_work = work_tables_mod.insert_work_rows(
        conn,
        table_name="stg_square_orders",
        run_id=run_id,
        rows=[
            work_tables_mod.WorkRow(
                source_ref=1,
                raw_payload={"id": "ord-100"},
                values={
                    "order_id": "ord-100",
                    "location_id": "LOC-1",
                    "customer_id": "cust-1",
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
                },
            )
        ],
    )

    inserted, duplicates = work_tables_mod.flush_work_table(
        conn,
        table_name="stg_square_orders",
        run_id=run_id,
    )

    assert inserted_into_work == 1
    assert inserted == 1
    assert duplicates == 0
