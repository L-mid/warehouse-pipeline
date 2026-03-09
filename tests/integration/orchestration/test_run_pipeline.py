from __future__ import annotations

import json

import psycopg

from warehouse_pipeline.orchestration import RunSpec, run_pipeline


def test_run_pipeline_happy_path(reinit_schema, dsn: str, run_artifacts_dir) -> None:
    """Pipeline run on smoke data works and returns expected."""

    manifest = run_pipeline(
        RunSpec(
            mode="snapshot",
            snapshot_key="smoke",
            runs_root=run_artifacts_dir,
        ),
        database_url=dsn,
    )


    assert manifest.status == "succeeded"

    manifest_path = run_artifacts_dir / str(manifest.run_id) / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "succeeded"   


    with psycopg.connect(dsn, autocommit=True) as conn:
        stg_customers = conn.execute(
            "SELECT COUNT(*) FROM stg_customers WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        stg_products = conn.execute(
            "SELECT COUNT(*) FROM stg_products WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        stg_orders = conn.execute(
            "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        stg_items = conn.execute(
            "SELECT COUNT(*) FROM stg_order_items WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        dq_rows = conn.execute(
            "SELECT COUNT(*) FROM dq_results WHERE run_id = %s",
            (manifest.run_id,),
        ).fetchone()[0]
        fact_orders_latest = conn.execute(
            "SELECT COUNT(*) FROM v_fact_orders_latest"
        ).fetchone()[0]

    assert stg_customers == 1
    assert stg_products == 1
    assert stg_orders == 1
    assert stg_items == 1
    assert dq_rows > 0
    assert fact_orders_latest == 1    