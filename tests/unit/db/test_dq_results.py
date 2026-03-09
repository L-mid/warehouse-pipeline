from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.dq_results import DQMetricRow, upsert_dq_results



def test_dq_results_happy_path() -> None:
    """
    Data quailty results writes a row. 
    And upsert only updates one row, not all rows over again.
    """

    conn = FakeConnection()
    run_id = uuid4()

    n = upsert_dq_results(
        conn,

        rows=[
            # add one row to insert.
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

    assert n == 1   # one row was inserted


    calls = [call for call in conn.calls if call[0] == "cursor.executemany"]    # called correctly
    # one call on the connection only
    assert len(calls) == 1
    assert len(calls[0][2]) == 1