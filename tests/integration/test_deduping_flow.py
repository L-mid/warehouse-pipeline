from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.dq.runner import run_dq



def test_deduping_duplicate_customers_are_rejected(conn, tmp_path: Path) -> None:
    """
    Duplicates should become `reject_rows` with `reason_code=duplicate_key`,
    and should not error.
    """
    p = tmp_path / "dupe_customers.csv"
    p.write_text(
        "Index,Customer Id,First Name,Last Name,Company,City,Country,Phone 1,Phone 2,Email,Subscription Date,Website\n"
        "1,dup,Ada,One,,,,,,,2020-01-01,\n"
        "2,dup,Ada,Two,,,,,,,2020-01-02,\n",
        encoding="utf-8",
    )

    summary = load_file(conn, input_path=p, table_name="stg_customers")
    run_dq(conn, run_id=summary.run_id, table_name=summary.table_name)

    assert summary.total == 2
    assert summary.loaded == 1
    assert summary.rejected == 1

    stg_ct = conn.execute("SELECT COUNT(*) FROM stg_customers WHERE run_id = %s", (summary.run_id,)).fetchone()[0]
    dup_rej_ct = conn.execute(
        """
        SELECT COUNT(*)
        FROM reject_rows
        WHERE run_id = %s AND table_name = 'stg_customers' AND reason_code = 'duplicate_key'
        """,
        (summary.run_id,),
    ).fetchone()[0]

    status = conn.execute(
        "SELECT status FROM ingest_runs WHERE run_id = %s AND table_name = %s",
        (summary.run_id, summary.table_name),
    ).fetchone()[0]

    assert int(stg_ct) == 1
    assert int(dup_rej_ct) == 1
    assert status == "succeeded"

    dq_dup_metric = conn.execute(
        """
        SELECT metric_value
        FROM dq_results
        WHERE run_id = %s AND table_name = %s AND metric_name = 'reason_code.duplicate_key.count'
        """,
        (summary.run_id, summary.table_name),
    ).fetchone()
    assert dq_dup_metric is not None