from __future__ import annotations

from uuid import uuid4

from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.writers.rejects import RejectInsert, insert_reject_rows



def test_insert_reject_rows_happy_path() -> None:
    """A reject row inserts in successfully."""
    conn = FakeConnection()
    run_id = uuid4()

    # inserts a good reject row
    n = insert_reject_rows(
        conn,
        run_id=run_id,
        rejects=[
            RejectInsert(
                table_name="stg_orders",
                source_ref=7,
                raw_payload={"id": 7},
                reason_code="bad_status",
                reason_detail="status not allowed",
            )
        ],
    )

    assert n == 1       # one call. 


    calls = [call for call in conn.calls if call[0] == "cursor.executemany"]    # correct call
    assert len(calls) == 1
    assert len(calls[0][2]) == 1

    