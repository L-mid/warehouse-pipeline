from __future__ import annotations

from uuid import uuid4

import warehouse_pipeline.stage.load as load_mod
from warehouse_pipeline.stage import StageReject, StageRow


def test_load_happy_path(monkeypatch) -> None:
    """Load groups rows by table, writes rejects, and returns summaries per table."""
    calls: list[tuple[str, str, int]] = []

    def fake_prepare_work_table(conn, *, table_name: str) -> None:
        """Append prepared `table_name`s."""
        calls.append(("prepare", table_name, 0))

    def fake_insert_work_rows(conn, *, table_name: str, run_id, rows) -> int:
        """Append inserted rows."""
        calls.append(("insert", table_name, len(rows)))
        return len(rows)

    def fake_flush_work_table(conn, *, table_name: str, run_id):
        """Append flushes."""
        calls.append(("flush", table_name, 0))
        return (len([c for c in calls if c[0] == "insert" and c[1] == table_name]), 0)

    def fake_insert_reject_rows(conn, *, run_id, rejects) -> int:
        """Append inserted rows."""
        calls.append(("rejects", "reject_rows", len(rejects)))
        return len(rejects)
    
    # re
    monkeypatch.setattr(load_mod, "prepare_work_table", fake_prepare_work_table)
    monkeypatch.setattr(load_mod, "insert_work_rows", fake_insert_work_rows)
    monkeypatch.setattr(load_mod, "flush_work_table", fake_flush_work_table)
    monkeypatch.setattr(load_mod, "insert_reject_rows", fake_insert_reject_rows)


    rows = [
        # two good rows
        StageRow(
            table_name="stg_customers",
            source_ref=1,
            raw_payload={"id": 1},
            values={"customer_id": 1, "full_name": "Ada Lovelace"},
        ),
        StageRow(
            table_name="stg_orders",
            source_ref=1,
            raw_payload={"id": 100},
            values={"order_id": 100, "customer_id": 1},
        ),
    ]
    rejects = [
        # one reject row
        StageReject(
            table_name="stg_order_items",
            source_ref=3,
            raw_payload={"id": 999},
            reason_code="unknown_product",
            reason_detail="missing product lookup",
        )
    ]

    results = load_mod.load_stage_rows(
        conn=object(),
        run_id=uuid4(),
        rows=rows,
        rejects=rejects,
    )

    assert ("rejects", "reject_rows", 1) in calls
    assert ("prepare", "stg_customers", 0) in calls 
    assert ("prepare", "stg_orders", 0) in calls

    assert results["stg_customers"].inserted_count == 1
    assert results["stg_orders"].inserted_count == 1
    assert results["stg_order_items"].explicit_reject_count == 1