from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, LiteralString

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
    """Collect diagnostic data from a run and return in a `debug` `dict`."""
    run_dirs = sorted(p for p in run_artifacts_dir.iterdir() if p.is_dir())
    latest_run_dir = run_dirs[-1] if run_dirs else None

    manifest = {}
    log_tail: list[str] = []
    if latest_run_dir is not None:
        manifest = _read_json(latest_run_dir / "manifest.json")
        log_tail = _tail_lines(latest_run_dir / "logs.jsonl", n=40)

    with psycopg.connect(dsn, autocommit=True) as conn:
        ledger_rows = conn.execute(
            """
            SELECT
                run_id::text,
                mode,
                COALESCE(snapshot_key, ''),
                status,
                COALESCE(error_message, '')
            FROM run_ledger
            ORDER BY started_at DESC
            LIMIT 5
            """
        ).fetchall()

        def _count(
            conn: psycopg.Connection[Any],
            query: LiteralString,
            params: Sequence[object] | None = None,
        ) -> int:
            """Run a `COUNT(*)` query safely, failing on a `None` row return."""
            row = conn.execute(query, params).fetchone()
            assert row is not None, f"COUNT query returned no rows: {query}"
            return int(row[0])

        table_counts = {
            "run_ledger": _count(conn, "SELECT COUNT(*) FROM run_ledger"),
            "stg_customers": _count(conn, "SELECT COUNT(*) FROM stg_customers"),
            "stg_products": _count(conn, "SELECT COUNT(*) FROM stg_products"),
            "stg_orders": _count(conn, "SELECT COUNT(*) FROM stg_orders"),
            "stg_order_items": _count(conn, "SELECT COUNT(*) FROM stg_order_items"),
            "reject_rows": _count(conn, "SELECT COUNT(*) FROM reject_rows"),
            "dq_results": _count(conn, "SELECT COUNT(*) FROM dq_results"),
            "dim_customer": _count(conn, "SELECT COUNT(*) FROM dim_customer"),
            "fact_orders": _count(conn, "SELECT COUNT(*) FROM fact_orders"),
            "fact_order_items": _count(conn, "SELECT COUNT(*) FROM fact_order_items"),
            "v_fact_orders_latest": _count(conn, "SELECT COUNT(*) FROM v_fact_orders_latest"),
        }

        dq_preview = conn.execute(
            """
            SELECT table_name, metric_name, passed, metric_value::text
            FROM dq_results
            ORDER BY table_name ASC, metric_name ASC
            LIMIT 20
            """
        ).fetchall()

        reject_preview = conn.execute(
            """
            SELECT table_name, reason_code, LEFT(reason_detail, 200)
            FROM reject_rows
            ORDER BY reject_id ASC
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
        "reject_preview": reject_preview,
    }


def _failure_blob(*, rc: int, debug: dict) -> str:
    """Formats a debuggable failure blob for these whole pipeline tests."""
    return (
        "pipeline CLI returned non-zero\n"
        f"rc={rc}\n"
        f"run_dirs={debug['run_dirs']}\n"
        f"manifest={json.dumps(debug['manifest'], indent=2, sort_keys=True)}\n"
        f"log_tail=\n" + "\n".join(debug["log_tail"]) + "\n"
        f"run_ledger_recent={debug['ledger_rows']}\n"
        f"table_counts={debug['table_counts']}\n"
        f"dq_preview={debug['dq_preview']}\n"
        f"reject_preview={debug['reject_preview']}\n"
    )


def _assert_artifacts_exist(manifest: dict) -> None:
    """Asserts that the key run artifacts exist on disk post run."""
    artifacts = manifest["artifacts"]
    assert Path(artifacts["run_dir"]).exists()
    assert Path(artifacts["manifest"]).exists()
    assert Path(artifacts["logs"]).exists()


@pytest.mark.docker_required
@pytest.mark.heavy_integration
def test_cli_run_pipeline_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """Run a full run from CLI to result code on smoke data."""
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    rc = main(
        [
            "run",
            "--mode",
            "snapshot",
            "--snapshot",
            "smoke",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)

    assert rc == 0, _failure_blob(rc=rc, debug=debug)  # formatted error data on failure

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "snapshot"
    assert manifest["snapshot_key"] == "smoke"
    _assert_artifacts_exist(manifest)

    assert debug["table_counts"]["reject_rows"] == 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] == 1
    assert debug["table_counts"]["fact_order_items"] == 1
    assert debug["table_counts"]["v_fact_orders_latest"] == 1

    # run success!


@pytest.mark.heavy_integration
@pytest.mark.non_ci
@pytest.mark.live_http
@pytest.mark.docker_required
def test_cli_run_pipeline_live_dummyjson_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """
    Run the full pipeline from CLI using the real live `DummyJSON` extraction path.
    Requires internet, CI does not run this test.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    rc = main(
        [
            "run",
            "--mode",
            "live",
            "--page-size",
            "100",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)

    assert rc == 0, _failure_blob(rc=rc, debug=debug)

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "live"
    assert manifest["snapshot_key"] is None
    _assert_artifacts_exist(manifest)

    # anything > 0 ok for live
    assert manifest["extract"]["counts"]["users"] > 0
    assert manifest["extract"]["counts"]["products"] > 0
    assert manifest["extract"]["counts"]["carts"] > 0

    # pages >= 1 is expected from extraction
    assert manifest["extract"]["pages_fetched"]["users"] >= 1
    assert manifest["extract"]["pages_fetched"]["products"] >= 1
    assert manifest["extract"]["pages_fetched"]["carts"] >= 1

    # in order to pass, dq checks must pass to avoid raise,
    # implicitly testing reject count for live mode is within tol

    # inner db and dq checks look non empty and ok
    assert debug["table_counts"]["stg_customers"] > 0
    assert debug["table_counts"]["stg_products"] > 0
    assert debug["table_counts"]["stg_orders"] > 0
    assert debug["table_counts"]["stg_order_items"] > 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] > 0
    assert debug["table_counts"]["fact_order_items"] > 0
    assert debug["table_counts"]["v_fact_orders_latest"] > 0
