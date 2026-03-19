from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from tests.integration.helpers.square import (
    square_line,
    square_order,
    square_tender,
    write_square_snapshot,
)
from warehouse_pipeline.orchestration.contract import RunSpec
from warehouse_pipeline.orchestration.runner import run_pipeline


def _smoke_orders() -> list[dict]:
    return [
        square_order(
            order_id="ord-100",
            state="COMPLETED",
            created_at="2026-03-10T10:00:00Z",
            updated_at="2026-03-10T10:05:00Z",
            closed_at="2026-03-10T10:05:00Z",
            total_money_cents=2300,
            net_total_money_cents=2000,
            total_discount_cents=200,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-espresso",
                    catalog_object_id="item-espresso",
                    name="Espresso",
                    quantity="1",
                    base_price_cents=1200,
                    gross_sales_cents=1200,
                    net_sales_cents=1200,
                ),
                square_line(
                    uid="line-cookie",
                    catalog_object_id="item-cookie",
                    name="Cookie",
                    quantity="2",
                    base_price_cents=400,
                    gross_sales_cents=800,
                    net_sales_cents=800,
                ),
            ],
            tenders=[
                square_tender(
                    tender_id="tender-100",
                    tender_type="CARD",
                    amount_cents=2300,
                    card_brand="VISA",
                )
            ],
        ),
        square_order(
            order_id="ord-101",
            state="COMPLETED",
            created_at="2026-03-10T11:00:00Z",
            updated_at="2026-03-10T11:05:00Z",
            closed_at="2026-03-10T11:05:00Z",
            total_money_cents=1100,
            net_total_money_cents=1000,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-bagel",
                    catalog_object_id="item-bagel",
                    name="Bagel",
                    quantity="1",
                    base_price_cents=1000,
                    gross_sales_cents=1000,
                    net_sales_cents=1000,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-101",
                    tender_type="CASH",
                    amount_cents=1100,
                )
            ],
        ),
    ]


@pytest.mark.docker_required
def test_integration_smoke_snapshot_run_pipeline_succeeds(
    reinit_schema,
    dsn: str,
    run_artifacts_dir: Path,
    tmp_path: Path,
) -> None:
    snapshot_root = tmp_path / "square_snapshot" / "smoke_v1"
    write_square_snapshot(snapshot_root, orders=_smoke_orders())

    manifest = run_pipeline(
        RunSpec(
            mode="snapshot",
            source_system="square_orders",
            snapshot_key="smoke_v1",
            snapshot_root=snapshot_root,
            runs_root=run_artifacts_dir,
            git_sha="test-sha",
            args_json={"test": "integration_smoke"},
        ),
        database_url=dsn,
    )

    assert manifest.status == "succeeded"
    assert manifest.extract["counts"] == {"orders": 2}
    assert manifest.stage["stg_square_orders"]["inserted_count"] == 2
    assert manifest.stage["stg_square_order_lines"]["inserted_count"] == 3
    assert manifest.stage["stg_square_tenders"]["inserted_count"] == 2
    assert manifest.gate["passed"] is True
    assert manifest.transform["files_ran"] == [
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    ]
    assert set(manifest.publish["metrics_available"]) == {
        "010_daily_sales_summary",
        "020_daily_sales_by_tender_type",
        "030_weekly_top_items",
        "040_daily_discount_summary",
        "050_daily_order_state_summary",
    }

    manifest_path = Path(manifest.artifacts["manifest"])
    logs_path = Path(manifest.artifacts["logs"])
    assert manifest_path.exists()
    assert logs_path.exists()
    assert "run_succeeded" in logs_path.read_text(encoding="utf-8")

    with psycopg.connect(dsn) as conn:
        run_status = conn.execute(
            "SELECT status FROM run_ledger WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        assert run_status == "succeeded"

        fact_orders_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
        latest_dq_count = conn.execute("SELECT COUNT(*) FROM v_dq_results_latest").fetchone()[0]

    assert fact_orders_count == 2
    assert latest_dq_count > 0
