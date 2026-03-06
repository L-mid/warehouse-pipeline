from __future__ import annotations

from datetime import date
from pathlib import Path

from warehouse_pipeline.db.ingest_runs import insert_ingest_run
from warehouse_pipeline.db.work_tables import WorkRow, finalize_work_to_staging, insert_work_rows, prepare_work_table



def test_prepare_work_table_creates_temp_table(conn) -> None:
    """Assert temp table exists when called."""
    prepare_work_table(conn, table_name="stg_customers")

    # the temp tables live in pg_temp schema
    exists = conn.execute("SELECT to_regclass('pg_temp._work_stg_customers') IS NOT NULL").fetchone()[0]
    assert bool(exists) is True


def test_insert_work_rows_happy_path(conn) -> None:
    """Row inserts successfully."""
    prepare_work_table(conn, table_name="stg_customers")

    run_id = insert_ingest_run(conn, input_path=Path("/tmp/customers.csv"), table_name="stg_customers")

    rows = [
        WorkRow(
            source_row=1,
            raw_payload={"raw": {"Customer Id": "abc"}},
            staging_mapping={
                "customer_id": "abc",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "full_name": "Ada Lovelace",
                "company": None,
                "city": None,
                "country": None,
                "phone_1": None,
                "phone_2": None,
                "email": None,
                "subscription_date": date(2020, 1, 1),
                "website": None,
            },
        )
    ]

    insert_work_rows(conn, table_name="stg_customers", run_id=run_id, rows=rows)

    ct = conn.execute("SELECT COUNT(*) FROM pg_temp._work_stg_customers WHERE run_id = %s", (run_id,)).fetchone()[0]
    assert int(ct) == 1



def test_finalize_work_to_staging_dedupes_and_rejects(conn) -> None:
    """Assert a vaild row passes but duplicate row on the same PK becomes a reject."""
    prepare_work_table(conn, table_name="stg_customers")

    run_id = insert_ingest_run(conn, input_path=Path("/tmp/customers.csv"), table_name="stg_customers")

    rows = [
        WorkRow(
            source_row=1,
            raw_payload={"raw": {"Customer Id": "dup"}},
            staging_mapping={
                "customer_id": "dup",
                "first_name": "Ada",
                "last_name": "One",
                "full_name": "Ada One",
                "company": None,
                "city": None,
                "country": None,
                "phone_1": None,
                "phone_2": None,
                "email": None,
                "subscription_date": date(2020, 1, 1),
                "website": None,
            },
        ),
        WorkRow(
            source_row=2,
            raw_payload={"raw": {"Customer Id": "dup"}},
            staging_mapping={
                "customer_id": "dup",
                "first_name": "Ada",
                "last_name": "Two",
                "full_name": "Ada Two",
                "company": None,
                "city": None,
                "country": None,
                "phone_1": None,
                "phone_2": None,
                "email": None,
                "subscription_date": date(2020, 1, 2),
                "website": None,
            },
        ),
    ]

    insert_work_rows(conn, table_name="stg_customers", run_id=run_id, rows=rows)

    inserted, dup_rejects = finalize_work_to_staging(conn, table_name="stg_customers", run_id=run_id)
    assert inserted == 1
    assert dup_rejects == 1

    stg_ct = conn.execute("SELECT COUNT(*) FROM stg_customers WHERE run_id = %s", (run_id,)).fetchone()[0]
    rej_ct = conn.execute(
        "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s AND table_name = 'stg_customers' AND reason_code = 'duplicate_key'",
        (run_id,),
    ).fetchone()[0]

    assert int(stg_ct) == 1
    assert int(rej_ct) == 1



def test_work_table_inherits_defaults_for_created_at(conn) -> None:
    """
    Temp table should be filled with defaults on creation
    and not raise on NULL mismatches.
    """

    prepare_work_table(conn, table_name="stg_customers")

    run_id = insert_ingest_run(conn, input_path=Path("/tmp/customers.csv"), table_name="stg_customers")

    rows = [
        WorkRow(
            source_row=1,
            raw_payload={"raw": {"Customer Id": "x"}},
            staging_mapping={
                "customer_id": "x",
                "first_name": "A",
                "last_name": "B",
                "full_name": "A B",
                "company": None,
                "city": None,
                "country": None,
                "phone_1": None,
                "phone_2": None,
                "email": None,
                "subscription_date": date(2020, 1, 1),
                "website": None,
            },
        )
    ]

    # Should not raise `NotNullViolation` on `created_at`
    insert_work_rows(conn, table_name="stg_customers", run_id=run_id, rows=rows)

    # And `created_at` should be populated by default/
    ok = conn.execute(
        "SELECT created_at IS NOT NULL FROM pg_temp._work_stg_customers WHERE run_id = %s LIMIT 1",
        (run_id,),
    ).fetchone()[0]
    assert bool(ok) is True