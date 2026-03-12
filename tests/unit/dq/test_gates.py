from __future__ import annotations

from typing import cast
from uuid import uuid4

import psycopg

from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.dq.gates import evaluate_stage_gates


def test_gates_happy_path() -> None:
    """Data quality gates run and pass good results."""
    run_id = uuid4()

    conn = FakeConnection(
        fetchone_rows=[
            # should pass
            ("snapshot",),
            (1,),
            (0,),
            (0,),  # stg_customers  row_count, dupes, reject_rate
            (1,),
            (0,),
            (0,),  # stg_products
            (1,),
            (0,),
            (0,),  # stg_orders
            (1,),
            (0,),
            (0,),  # stg_order_items
            (0,),  # stg_orders missing_customers.count
            (0,),  # stg_order_items missing_products.count
            (0,),  # stg_order_items orphan_orders.count
        ]
    )
    conn = cast(psycopg.Connection[tuple], conn)

    decision = evaluate_stage_gates(conn, run_id=run_id)

    assert decision.run_id == run_id
    assert decision.mode == "snapshot"
    assert decision.passed is True
    assert decision.failures == ()  # none
    assert decision.warnings == ()  # none
