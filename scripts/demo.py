import os
import time

import psycopg

# dummy implementation, should integrate with main piepline later

def wait_for_db(dsn: str, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    raise RuntimeError(f"DB not ready: {dsn}. Last error: {last_err}")


def main() -> None:
    dsn = os.getenv("WAREHOUSE_TEST_DSN", "postgresql://postgres:postgres@localhost:5433/warehouse")
    wait_for_db(dsn)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename;
                """
            )
            tables = [r[0] for r in cur.fetchall()]

    print("✅ Postgres is reachable")
    print("✅ Tables in public schema:")
    for t in tables:
        print(f"  - {t}")

    needed = {"ingest_runs", "stg_customers", "stg_retail_transactions", "reject_rows"}
    missing = sorted(list(needed - set(tables)))
    if missing:
        raise SystemExit(f"Missing expected tables: {missing}")


if __name__ == "__main__":
    main()