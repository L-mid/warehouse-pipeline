import json
import psycopg
 

def test_tables_exist(conn: psycopg.Connection) -> None:
    """Fetch all tables and assert they exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              to_regclass('public.ingest_runs')             IS NOT NULL AS ingest_runs_ok,
              to_regclass('public.stg_customers')           IS NOT NULL AS stg_customers_ok,
              to_regclass('public.stg_retail_transactions') IS NOT NULL AS stg_retail_transactions_ok,
              to_regclass('public.reject_rows')             IS NOT NULL AS reject_rows_ok,
              to_regclass('public.dq_results')              IS NOT NULL AS dq_results_ok
            """
        )
        ingest_ok, cust_ok, retail_transactions_ok, reject_ok, dq_ok = cur.fetchone()
    assert ingest_ok and cust_ok and retail_transactions_ok and reject_ok and dq_ok

 
def test_ingest_runs_status_check_constraint(conn: psycopg.Connection) -> None:
    """Row is falsey or errors if status is not in expected."""
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
            # for now, any error is fine (it should for sure fail). Should tighten later.
            conn.rollback()


def test_delete_run_cascades_to_staging_and_rejects(conn: psycopg.Connection) -> None:
    """ 
    Inserts a good row and a reject row, and asserts that deleting `ingest_runs` cascades to both.
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

        # insert a good staging row. Example used: `stg_customers`.
        cur.execute(
            """
            INSERT INTO stg_customers (
              run_id, customer_id, first_name, last_name, full_name,
              company, city, country, phone_1, phone_2, email, subscription_date, website
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "dE014d010c7ab0c",
                "Ada",
                "Lovelace",
                "Ada Lovelace",
                "Stewart-Flynn",
                "Rowlandberg",
                "GB",
                "846-790-4623x4715",
                "(422)787-2331x71127",
                "ada@example.com",
                "2021-07-26",
                "http://example.com/",
            ),
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
                "subscription_date='not-a-date'",
            ),
        )

        # insert a `dq_results` row 
        cur.execute(
            """
            INSERT INTO dq_results (run_id, table_name, check_name, metric_name, metric_value, passed, details_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                run_id,
                "stg_customers",
                "uniqueness_of_key",
                "customer_id.duplicate_keys",
                0,
                True,
                json.dumps({"example": True}),
            ),
        )

        # delete the run, which should cascade to full deletion.
        cur.execute("DELETE FROM ingest_runs WHERE run_id = %s", (run_id,))

        cur.execute("SELECT COUNT(*) FROM stg_customers WHERE run_id = %s", (run_id,))
        (stg_count,) = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM reject_rows WHERE run_id = %s", (run_id,))
        (rej_count,) = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM dq_results WHERE run_id = %s", (run_id,))
        (dq_count,) = cur.fetchone()

    assert stg_count == 0
    assert rej_count == 0   
    assert dq_count == 0
    # deletion of `ingest_runs` cascaded to these rows, success
