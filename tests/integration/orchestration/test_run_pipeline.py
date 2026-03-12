from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, LiteralString

import psycopg
import pytest

from warehouse_pipeline.orchestration import RunSpec, run_pipeline


@pytest.mark.docker_required
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

    def _count(
        conn: psycopg.Connection[Any],
        query: LiteralString,
        params: Sequence[object] | None = None,
    ) -> int:
        """Run a COUNT(*) query safely, failing loudly if no row is returned."""
        row = conn.execute(query, params).fetchone()
        assert row is not None, f"COUNT query returned no rows: {query}"
        return int(row[0])

    with psycopg.connect(dsn, autocommit=True) as conn:
        stg_customers = _count(
            conn,
            "SELECT COUNT(*) FROM stg_customers WHERE run_id = %s",
            (manifest.run_id,),
        )
        stg_products = _count(
            conn,
            "SELECT COUNT(*) FROM stg_products WHERE run_id = %s",
            (manifest.run_id,),
        )
        stg_orders = _count(
            conn,
            "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
            (manifest.run_id,),
        )
        stg_items = _count(
            conn,
            "SELECT COUNT(*) FROM stg_order_items WHERE run_id = %s",
            (manifest.run_id,),
        )
        dq_rows = _count(
            conn,
            "SELECT COUNT(*) FROM dq_results WHERE run_id = %s",
            (manifest.run_id,),
        )
        fact_orders_latest = _count(
            conn,
            "SELECT COUNT(*) FROM v_fact_orders_latest",
        )

    assert stg_customers == 1
    assert stg_products == 1
    assert stg_orders == 1
    assert stg_items == 1
    assert dq_rows > 0
    assert fact_orders_latest == 1
