from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping
from uuid import UUID

from psycopg import Connection, sql

from warehouse_pipeline.db.dq_results import DQMetricRow, delete_dq_results, insert_dq_results

_ALLOWED_TABLES: dict[str, dict[str, str]] = {
    # the "uniqueness of key" within a run uses this key column:
    "stg_customers": {"key_col": "customer_id"},
    "stg_retail_transactions": {"key_col": "source_row"},
}


_Q6 = Decimal("0.000000")  # this one is align with numeric(18,6), for higher precison.



def _q6(x: Decimal) -> Decimal:
    """Normalalize any given `Decimal` to Q6 (`0.000000`) for Postgres."""
    return x.quantize(_Q6, rounding=ROUND_HALF_UP)


def _ensure_run_succeeded(conn: Connection, *, run_id: UUID, table_name: str) -> None:
    """
    Ensure a run succeeded having a given `run_id` and `table_name`. 
    
    Raise on a row being `None`, or a row's `status` not being `succeeded`.
    Return `None` on success.
    """
    row = conn.execute(     # extract this row
        "SELECT status FROM ingest_runs WHERE run_id = %s AND table_name = %s",
        (run_id, table_name),
    ).fetchone()
    if row is None:
        raise ValueError(f"run_id not found for table: run_id={run_id} table={table_name}")
    (status,) = row
    if status != "succeeded":
        raise ValueError(f"run is not succeeded: run_id={run_id} table={table_name} status={status}")



def _get_not_null_columns(conn: Connection, *, table_name: str) -> list[str]:
    """
    Returns all columns that are NOT NULL in the target table provided by `table_name`.

    Uses public schema.
    """
    # Using pg_catalog avoids information_schema quirks and is fast.
    rows = conn.execute(
        """
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attnotnull
        ORDER BY a.attnum
        """,
        (table_name,),
    ).fetchall()
    return [r[0] for r in rows]     # return all rows


def _count_total_rows(conn: Connection, *, table_name: str, run_id: UUID) -> int:
    """Counts every row using a provided `table_name` and `run_id`. Returns count as `int`."""
    q = sql.SQL("SELECT COUNT(*) FROM {t} WHERE run_id = %s").format(t=sql.Identifier(table_name))
    return int(conn.execute(q, (run_id,)).fetchone()[0])



def _check_uniqueness_of_key(conn: Connection, *, run_id: UUID, table_name: str) -> Iterable[DQMetricRow]:
    """Ensure each table's keys are all unique. Yields a metric row."""
    key_col = _ALLOWED_TABLES[table_name]["key_col"]

    # Counts how many keys have duplicates (COUNT(*) > 1) within the run.
    q = sql.SQL(
        """
        SELECT COUNT(*) FROM (
          SELECT {k}
          FROM {t}
          WHERE run_id = %s
          GROUP BY {k}
          HAVING COUNT(*) > 1
        ) d
        """
    ).format(t=sql.Identifier(table_name), k=sql.Identifier(key_col))

    dup_keys = int(conn.execute(q, (run_id,)).fetchone()[0])

    # for now: strict 0 gate for this clean data.
    # later: warn on non suceeded runs, assess why, etc.
    passed = (dup_keys == 0)

    yield DQMetricRow(
        run_id=run_id,
        table_name=table_name,
        check_name="uniqueness_of_key",         
        metric_name=f"{key_col}.duplicate_keys",
        metric_value=_q6(Decimal(dup_keys)),
        passed=passed,
        details={"key_col": key_col, "duplicate_keys": dup_keys},
    )




def _check_null_rate_required(conn: Connection, *, run_id: UUID, table_name: str) -> Iterable[DQMetricRow]:
    """
    Check null rates are as required/expected. 
    Yields metrics rows for `null_count` and `null_rate`.
    """
    total = _count_total_rows(conn, table_name=table_name, run_id=run_id)
    cols = _get_not_null_columns(conn, table_name=table_name)

    for col in cols:
        q = sql.SQL(
            "SELECT COUNT(*) FROM {t} WHERE run_id = %s AND {c} IS NULL"
        ).format(t=sql.Identifier(table_name), c=sql.Identifier(col))
        null_count = int(conn.execute(q, (run_id,)).fetchone()[0])

        null_rate = Decimal(0)
        if total > 0:
            null_rate = Decimal(null_count) / Decimal(total)

        passed = (null_count == 0)

        # count metric
        yield DQMetricRow(
            run_id=run_id,
            table_name=table_name,
            check_name="null_rate_required",
            metric_name=f"{col}.null_count",
            metric_value=_q6(Decimal(null_count)),
            passed=passed,
            details={"col": col, "total_rows": total, "null_count": null_count},
        )
        # rate metric
        yield DQMetricRow(
            run_id=run_id,
            table_name=table_name,
            check_name="null_rate_required",
            metric_name=f"{col}.null_rate",
            metric_value=_q6(Decimal(null_rate)),
            passed=passed,
            details={"col": col, "total_rows": total, "null_count": null_count},
        )


def _check_invalid_type_counts(conn: Connection, *, run_id: UUID, table_name: str) -> Iterable[DQMetricRow]:
    """
    Checks `reject_rows` grouped by `reason_code` for the same run/table.

    Yields:
    - `reject_rows.total`
    - `reject_rows.reject_rate` (vs total staged rows)
    - `reason_code.<code>.count` for each code observed
    """
    total_staged = _count_total_rows(conn, table_name=table_name, run_id=run_id)

    rows = conn.execute(
        """
        SELECT reason_code, COUNT(*) AS n
        FROM reject_rows
        WHERE run_id = %s AND table_name = %s
        GROUP BY 1
        ORDER BY n DESC, reason_code ASC
        """,
        (run_id, table_name),
    ).fetchall()

    total_rejects = sum(int(r[1]) for r in rows)
    reject_rate = Decimal(0)
    if total_staged > 0:
        # the rate of rejection
        reject_rate = Decimal(total_rejects) / Decimal(total_staged)

    # rollups stats
    passed_total = (total_rejects == 0)
    # total
    yield DQMetricRow(
        run_id=run_id,
        table_name=table_name,
        check_name="invalid_type_counts",
        metric_name="reject_rows.total",
        metric_value=_q6(Decimal(total_rejects)),
        passed=passed_total,
        details={"total_rejects": total_rejects, "total_staged": total_staged},
    )
    # rate
    yield DQMetricRow(
        run_id=run_id,
        table_name=table_name,
        check_name="invalid_type_counts",
        metric_name="reject_rows.reject_rate",
        metric_value=_q6(Decimal(reject_rate)),
        passed=passed_total,
        details={"total_rejects": total_rejects, "total_staged": total_staged},
    )

    # some stats per `reason_code`
    for reason_code, n in rows:
        n_int = int(n)
        passed = (n_int == 0)
        yield DQMetricRow(
            run_id=run_id,
            table_name=table_name,
            check_name="invalid_type_counts",
            metric_name=f"reason_code.{reason_code}.count",
            metric_value=_q6(Decimal(n_int)),
            passed=passed,
            details={"reason_code": reason_code, "count": n_int},
        )


def run_dq(conn: Connection, *, run_id: UUID, table_name: str) -> int:
    """
    Runs all DQ checks for a provided (`run_id`, `table_name`) pair, 
    and finally inserts the derived metric rows into the DB's `dq_results` table.

    Idempotenty technique:
    - pre-deletes existing `dq_results` for (`run_id`, `table_name`). Does not make duplicates of itself.
    - inserts a full set of rows for this run/table post running and collection.

    Returns: `inserted` (an `int` count of all inserted rows).
    """
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"unsupported table_name for dq: {table_name}")

    _ensure_run_succeeded(conn, run_id=run_id, table_name=table_name)

    # one transaction. uses `delete+insert` so results are "all at once"
    delete_dq_results(conn, run_id=run_id, table_name=table_name)

    metrics: list[DQMetricRow] = []
    metrics.extend(_check_uniqueness_of_key(conn, run_id=run_id, table_name=table_name))
    metrics.extend(_check_null_rate_required(conn, run_id=run_id, table_name=table_name))
    metrics.extend(_check_invalid_type_counts(conn, run_id=run_id, table_name=table_name))
    # all metrics collected

    # finally: insert all derived metrics rows into the DB proper
    inserted = insert_dq_results(conn, rows=metrics)
    conn.commit()
    return inserted



