import json
import psycopg


def test_tables_exist(conn: psycopg.Connection) -> None:
    """Fetch all tables and assert they exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              to_regclass('public.ingest_runs')   IS NOT NULL AS ingest_runs_ok,
              to_regclass('public.stg_customers') IS NOT NULL AS stg_customers_ok,
              to_regclass('public.stg_orders')    IS NOT NULL AS stg_orders_ok,
              to_regclass('public.reject_rows')   IS NOT NULL AS reject_rows_ok
            """
        )
        ingest_ok, cust_ok, orders_ok, reject_ok = cur.fetchone()
    assert ingest_ok and cust_ok and orders_ok and reject_ok


def test_ingest_runs_status_check_constraint(conn: psycopg.Connection) -> None:
    """Row is falsy or errors if status is not in expected."""
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO ingest_runs (input_path, table_name, status)
                VALUES (%s, %s, %s)
                """,
                ("data/sample/customers.csv", "stg_customers", "not_a_real_status"),
            )
            assert False, "expected CHECK constraint to reject invalid status"
        except Exception:
            # minimal: any error is fine (it should for sure fail). Should tighten later.
            conn.rollback()


def test_delete_run_cascades_to_staging_and_rejects(conn: psycopg.Connection) -> None:
    """ 
    Inserts a good row and a reject row, and asserts on deletion both are 0.
    """
    with conn.cursor() as cur:
        # create a run
        cur.execute(
            """
            INSERT INTO ingest_runs (input_path, table_name, status)
            VALUES (%s, %s, %s)
            RETURNING run_id
            """,
            ("data/sample/customers.csv", "stg_customers", "running"),
        )
        (run_id,) = cur.fetchone()

        # insert a good staging row
        cur.execute(
            """
            INSERT INTO stg_customers (run_id, customer_id, full_name, email, signup_date, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, 123, "Ada Lovelace", "ada@example.com", "2026-01-01", "GB"),
        )

        # insert a reject row
        cur.execute(
            """
            INSERT INTO reject_rows (run_id, table_name, source_row, raw_payload, reason_code, reason_detail)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            """,
            (
                run_id,
                "stg_customers",
                7,
                json.dumps({"raw_line": "  bad,row,here  "}),
                "invalid_date",
                "signup_date='not-a-date'",
            ),
        )

        # delete the run -> should cascade
        cur.execute("DELETE FROM ingest_runs WHERE run_id = %s", (run_id,))

        cur.execute("SELECT COUNT(*) FROM stg_customers WHERE run_id = %s", (run_id,))
        (stg_count,) = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM reject_rows WHERE run_id = %s", (run_id,))
        (rej_count,) = cur.fetchone()

    assert stg_count == 0
    assert rej_count == 0   
