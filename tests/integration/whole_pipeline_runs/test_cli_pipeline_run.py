from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from warehouse_pipeline.cli.main import main




def _read_json(path: Path) -> dict:
    """Read `.json` from provided path."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _tail_lines(path: Path, *, n: int = 20) -> list[str]:
    """Show read lines from provided `path`, default limit is 20 lines."""
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[-n:]

    
def _collect_pipeline_debug(*, dsn: str, run_artifacts_dir: Path) -> dict:
    """Collect data on failure and return in a `dict`."""
    run_dirs = sorted(p for p in run_artifacts_dir.iterdir() if p.is_dir())
    latest_run_dir = run_dirs[-1] if run_dirs else None

    manifest = {}
    log_tail: list[str] = []
    if latest_run_dir is not None:
        manifest = _read_json(latest_run_dir / "manifest.json")
        log_tail = _tail_lines(latest_run_dir / "logs.jsonl", n=25)


    with psycopg.connect(dsn, autocommit=True) as conn:
        ledger_rows = conn.execute(
            """
            SELECT run_id::text, status, snapshot_key, COALESCE(error_message, '')
            FROM run_ledger
            ORDER BY started_at DESC
            LIMIT 5
            """
        ).fetchall()

        table_counts = {
            "run_ledger": conn.execute("SELECT COUNT(*) FROM run_ledger").fetchone()[0],
            "stg_customers": conn.execute("SELECT COUNT(*) FROM stg_customers").fetchone()[0],
            "stg_products": conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0],
            "stg_orders": conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0],
            "stg_order_items": conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0],
            "reject_rows": conn.execute("SELECT COUNT(*) FROM reject_rows").fetchone()[0],
            "dq_results": conn.execute("SELECT COUNT(*) FROM dq_results").fetchone()[0],
            "dim_customer": conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0],
            "fact_orders": conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0],
            "fact_order_items": conn.execute("SELECT COUNT(*) FROM fact_order_items").fetchone()[0],
            "v_fact_orders_latest": conn.execute("SELECT COUNT(*) FROM v_fact_orders_latest").fetchone()[0],
        }

        dq_preview = conn.execute(
            """
            SELECT table_name, metric_name, passed, metric_value::text
            FROM dq_results
            ORDER BY table_name ASC, metric_name ASC
            LIMIT 20
            """
        ).fetchall()

    return {
        "run_dirs": [p.name for p in run_dirs],
        "manifest": manifest,
        "log_tail": log_tail,
        "ledger_rows": ledger_rows,
        "table_counts": table_counts,
        "dq_preview": dq_preview,
    }    


@pytest.mark.heavy_integration
def test_cli_run_pipeline_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """Run a full run from CLI to result code. Runs on smoke data only for now."""
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)   


    rc = main(
        [
            "run",
            "--mode", "snapshot",
            "--snapshot", "smoke",
            "--runs-root", str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)

    assert rc == 0, (       # formatted error data on failure
        "pipeline CLI returned non-zero\n"
        f"rc={rc}\n"
        f"run_dirs={debug['run_dirs']}\n"
        f"manifest={json.dumps(debug['manifest'], indent=2, sort_keys=True)}\n"
        f"log_tail=\n" + "\n".join(debug["log_tail"]) + "\n"
        f"run_ledger_recent={debug['ledger_rows']}\n"
        f"table_counts={debug['table_counts']}\n"
        f"dq_preview={debug['dq_preview']}\n"
    )


    assert debug["manifest"]["status"] == "succeeded"
    assert debug["table_counts"]["reject_rows"] == 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] == 1
    assert debug["table_counts"]["fact_order_items"] == 1
    assert debug["table_counts"]["v_fact_orders_latest"] == 1


