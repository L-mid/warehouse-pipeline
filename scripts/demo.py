from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import psycopg


DEFAULT_DSN = os.getenv(
    "WAREHOUSE_DSN",
    "postgresql://postgres:postgres@localhost:5433/warehouse",
)


# stub implementation improve later
def wait_for_db(dsn: str, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None

    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return
        except Exception as exc:
            last_err = exc
            time.sleep(0.25)

    raise RuntimeError(f"DB not ready: {dsn}. Last error: {last_err}")


def run_cmd(cmd: list[str], *, cwd: Path) -> str:
    print(f"$ {' '.join(cmd)}")
    completed = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )

    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)

    if completed.returncode != 0:
        raise SystemExit(
            f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}"
        )

    return completed.stdout


def fetch_one_value(conn: psycopg.Connection, sql_text: str, params: tuple = ()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql_text, params)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Expected one row but query returned none")
    return int(row[0])


def fetch_run_ledger_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM run_ledger")
        row = cur.fetchone()

    if row is None:
        raise RuntimeError("Expected COUNT(*) row from run_ledger")

    return int(row[0])



def fetch_latest_run(conn: psycopg.Connection) -> tuple[UUID, str, str, str | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, status, mode, snapshot_key
            FROM run_ledger
            ORDER BY started_at DESC, run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError("No run found in run_ledger after pipeline run")

    run_id, status, mode, snapshot_key = row
    return UUID(str(run_id)), str(status), str(mode), snapshot_key



def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runs_root = repo_root / "runs"

    wait_for_db(DEFAULT_DSN)

    run_cmd(
        [sys.executable, "-m", "warehouse_pipeline.cli.main", "db", "init"],
        cwd=repo_root,
    )

    with psycopg.connect(DEFAULT_DSN) as conn:
        before_run_count = fetch_run_ledger_count(conn)

    run_cmd(
        [
            sys.executable,
            "-m",
            "warehouse_pipeline.cli.main",
            "run",
            "--mode",
            "snapshot",
            "--snapshot",
            "smoke",
        ],
        cwd=repo_root,
    )

    with psycopg.connect(DEFAULT_DSN) as conn:
        after_run_count = fetch_run_ledger_count(conn)
        run_id, status, mode, snapshot_key = fetch_latest_run(conn)

        if after_run_count <= before_run_count:
            raise SystemExit(
                "Expected pipeline run to add a row to run_ledger "
                f"(before={before_run_count}, after={after_run_count})"
            )

        if status != "succeeded":
            raise SystemExit(f"Expected succeeded run, got status={status!r}")
        if mode != "snapshot":
            raise SystemExit(f"Expected snapshot mode, got mode={mode!r}")
        if snapshot_key != "smoke":
            raise SystemExit(f"Expected snapshot_key='smoke', got {snapshot_key!r}")

        stage_customers = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM stg_customers WHERE run_id = %s",
            (run_id,),
        )
        stage_products = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM stg_products WHERE run_id = %s",
            (run_id,),
        )
        stage_orders = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM stg_orders WHERE run_id = %s",
            (run_id,),
        )
        stage_order_items = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM stg_order_items WHERE run_id = %s",
            (run_id,),
        )

        dq_metric_rows = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM dq_results WHERE run_id = %s",
            (run_id,),
        )
        dq_failed_rows = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM dq_results WHERE run_id = %s AND passed = FALSE",
            (run_id,),
        )

        dim_customers = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM dim_customer WHERE source_run_id = %s",
            (run_id,),
        )
        fact_orders = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM fact_orders WHERE source_run_id = %s",
            (run_id,),
        )
        fact_order_items = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM fact_order_items WHERE source_run_id = %s",
            (run_id,),
        )

        latest_dim_customers = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM v_dim_customer_latest",
        )
        latest_fact_orders = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM v_fact_orders_latest",
        )
        latest_fact_order_items = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM v_fact_order_items_latest",
        )
        latest_dq_rows = fetch_one_value(
            conn,
            "SELECT COUNT(*) FROM v_dq_results_latest",
        )

    manifest_path = runs_root / str(run_id) / "manifest.json"
    logs_path = runs_root / str(run_id) / "logs.jsonl"

    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest artifact: {manifest_path}")
    if not logs_path.exists():
        raise SystemExit(f"Missing logs artifact: {logs_path}")

    if stage_customers <= 0:
        raise SystemExit("Expected stg_customers to have rows for demo run")
    if stage_products <= 0:
        raise SystemExit("Expected stg_products to have rows for demo run")
    if stage_orders <= 0:
        raise SystemExit("Expected stg_orders to have rows for demo run")
    if stage_order_items <= 0:
        raise SystemExit("Expected stg_order_items to have rows for demo run")
    if dq_metric_rows <= 0:
        raise SystemExit("Expected dq_results to have rows for demo run")
    if dq_failed_rows != 0:
        raise SystemExit(
            f"Expected all DQ metrics to pass for smoke snapshot, got {dq_failed_rows} failures"
        )
    if dim_customers <= 0:
        raise SystemExit("Expected dim_customer rows for demo run")
    if fact_orders <= 0:
        raise SystemExit("Expected fact_orders rows for demo run")
    if fact_order_items <= 0:
        raise SystemExit("Expected fact_order_items rows for demo run")
    if latest_dim_customers <= 0:
        raise SystemExit("Expected v_dim_customer_latest to be queryable")
    if latest_fact_orders <= 0:
        raise SystemExit("Expected v_fact_orders_latest to be queryable")
    if latest_fact_order_items <= 0:
        raise SystemExit("Expected v_fact_order_items_latest to be queryable")
    if latest_dq_rows <= 0:
        raise SystemExit("Expected v_dq_results_latest to be queryable")

    print()
    print("✅ Demo passed")
    print(f"   run_id:            {run_id}")
    print(f"   stg_customers:     {stage_customers}")
    print(f"   stg_products:      {stage_products}")
    print(f"   stg_orders:        {stage_orders}")
    print(f"   stg_order_items:   {stage_order_items}")
    print(f"   dq_results:        {dq_metric_rows}")
    print(f"   dim_customer:      {dim_customers}")
    print(f"   fact_orders:       {fact_orders}")
    print(f"   fact_order_items:  {fact_order_items}")
    print(f"   manifest:          {manifest_path}")
    print(f"   logs:              {logs_path}")


if __name__ == "__main__":
    main()