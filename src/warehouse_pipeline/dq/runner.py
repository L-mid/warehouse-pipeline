from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping
from uuid import UUID

from psycopg import Connection, sql

from warehouse_pipeline.db.dq_results import DQMetricRow, delete_dq_results, upsert_dq_results
from warehouse_pipeline.db.writers.staging import TABLE_SPECS, StagingTableSpec


_Q6 = Decimal("0.000000")  # this one is align with numeric(18,6), for higher precison.


@dataclass(frozen=True)
class DQRunSummary:
    """
    Summary of each table's DQ execution.
    """
    run_id: UUID
    table_name: str
    metrics_written: int
    failed_metrics: int
    passed: bool            # for gating later



def _q6(value: Decimal | int | str) -> Decimal:
    """Normalize a numeric value to the `numeric(18,6)` shape used by `dq_results`."""
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(_Q6, rounding=ROUND_HALF_UP)


def _ensure_run_exists(conn: Connection, *, run_id: UUID) -> None:
    """
    Raise if the provided `run_id` is missing from `run_ledger`.
    Return `None` on success.
    """
    row = conn.execute(
        """
        SELECT 1
        FROM run_ledger
        WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"run_id not found in run_ledger: {run_id}")


def _count_rows(conn: Connection, *, table_name: str, run_id: UUID) -> int:
    """Count all staged rows for one provided `(run_id, table_name)` pair."""
    q = sql.SQL(
        """
        SELECT COUNT(*)
        FROM {table_name}
        WHERE run_id = %s
        """
    ).format(table_name=sql.Identifier(table_name))
    row = conn.execute(q, (run_id,)).fetchone()
    assert row is not None
    return int(row[0])


def _count_duplicate_keys(
    conn: Connection,
    *,
    spec: StagingTableSpec,
    run_id: UUID,
) -> int:
    """
    Count how many distinct key groups are duplicated inside one staged run.
    """
    key_select = sql.SQL(", ").join(sql.Identifier(col) for col in spec.key_cols)

    # Example, if `(run_id, order_id)` appears three times for the same order,
    # that contributes one duplicate key group, not two extra rows.

    q = sql.SQL(
        """
        SELECT COUNT(*)
        FROM (
            SELECT {key_select}
            FROM {table_name}
            WHERE run_id = %s
            GROUP BY {key_select}
            HAVING COUNT(*) > 1
        ) AS dup_keys
        """
    ).format(
        table_name=sql.Identifier(spec.table_name),
        key_select=key_select,
    )

    row = conn.execute(q, (run_id,)).fetchone()
    assert row is not None
    return int(row[0])


def _reject_counts_by_reason(
    conn: Connection,
    *,
    run_id: UUID,
    table_name: str,
) -> list[tuple[str, int]]:
    """Return reject counts per `reason_code` for this `(run_id, table_name)`."""
    rows = conn.execute(
        """
        SELECT reason_code, COUNT(*) AS n
        FROM reject_rows
        WHERE run_id = %s
          AND table_name = %s
        GROUP BY reason_code
        ORDER BY n DESC, reason_code ASC
        """,
        (run_id, table_name),
    ).fetchall()
    return [(str(reason_code), int(n)) for reason_code, n in rows]


def _count_missing_customer_orders(conn: Connection, *, run_id: UUID) -> int:
    """
    Count staged orders whose `customer_id` does not exist in `stg_customers`
    for the same run.
    """
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_orders o
        LEFT JOIN stg_customers c
          ON c.run_id = o.run_id
         AND c.customer_id = o.customer_id
        WHERE o.run_id = %s
          AND c.customer_id IS NULL
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _count_missing_product_items(conn: Connection, *, run_id: UUID) -> int:
    """
    Count staged order items whose `product_id` does not exist in
    `stg_products` for the same run.
    """
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_order_items i
        LEFT JOIN stg_products p
          ON p.run_id = i.run_id
         AND p.product_id = i.product_id
        WHERE i.run_id = %s
          AND i.product_id IS NOT NULL
          AND p.product_id IS NULL
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _count_orphan_order_items(conn: Connection, *, run_id: UUID) -> int:
    """
    Count staged order items whose `order_id` does not exist in `stg_orders`
    for the same run.
    """
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_order_items i
        LEFT JOIN stg_orders o
          ON o.run_id = i.run_id
         AND o.order_id = i.order_id
        WHERE i.run_id = %s
          AND o.order_id IS NULL
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _metric(
    *,
    run_id: UUID,
    table_name: str,
    check_name: str,
    metric_name: str,
    metric_value: Decimal | int | str,
    passed: bool,
    details_json: dict,
) -> DQMetricRow:
    """Constructor helper to keep all rows consistent."""
    return DQMetricRow(
        run_id=run_id,
        table_name=table_name,
        check_name=check_name,
        metric_name=metric_name,
        metric_value=_q6(metric_value),
        passed=passed,
        details_json=details_json,
    )



def _build_volume_and_reject_metrics(
    conn: Connection,
    *,
    spec: StagingTableSpec,
    run_id: UUID,
) -> list[DQMetricRow]:
    """
    Builds:
    - `row_count`
    - `duplicate_keys.count`
    - `reject_rows.total`
    - `reject_rows.reject_rate`
    - `reason_code.<code>.count`
    """
    table_name = spec.table_name
    total_rows = _count_rows(conn, table_name=table_name, run_id=run_id)
    duplicate_keys = _count_duplicate_keys(conn, spec=spec, run_id=run_id)
    reject_rows_by_reason = _reject_counts_by_reason(conn, run_id=run_id, table_name=table_name)
    total_rejects = sum(n for _, n in reject_rows_by_reason)

    # Defined as rejects / staged_rows.
    # When staged_rows == 0, keep the rate at 0 rather than divide by zero.
    reject_rate = Decimal("0")
    if total_rows > 0:
        reject_rate = Decimal(total_rejects) / Decimal(total_rows)

    rows: list[DQMetricRow] = []

    rows.append(
        _metric(
            run_id=run_id,
            table_name=table_name,
            check_name="stage_volume",
            metric_name="row_count",
            metric_value=total_rows,
            passed=(total_rows > 0),
            details_json={"row_count": total_rows},
        )
    )

    rows.append(
        _metric(
            run_id=run_id,
            table_name=table_name,
            check_name="stage_keys",
            metric_name="duplicate_keys.count",
            metric_value=duplicate_keys,
            passed=(duplicate_keys == 0),
            details_json={
                "key_cols": list(spec.key_cols),
                "duplicate_key_groups": duplicate_keys,
            },
        )
    )


    rows.append(
        _metric(
            run_id=run_id,
            table_name=table_name,
            check_name="stage_rejects",
            metric_name="reject_rows.total",
            metric_value=total_rejects,
            passed=(total_rejects == 0),
            details_json={
                "total_rejects": total_rejects,
                "row_count": total_rows,
            },
        )
    )


    rows.append(
        _metric(
            run_id=run_id,
            table_name=table_name,
            check_name="stage_rejects",
            metric_name="reject_rows.reject_rate",
            metric_value=reject_rate,
            passed=(reject_rate == Decimal("0")),
            details_json={
                "total_rejects": total_rejects,
                "row_count": total_rows,
                "formula": "total_rejects / row_count",
            },
        )
    )


    for reason_code, count in reject_rows_by_reason:
        rows.append(
            _metric(
                run_id=run_id,
                table_name=table_name,
                check_name="stage_rejects",
                metric_name=f"reason_code.{reason_code}.count",
                metric_value=count,
                passed=(count == 0),
                details_json={
                    "reason_code": reason_code,
                    "count": count,
                },
            )
        )

    return rows


def _build_relation_metrics(conn: Connection, *, table_name: str, run_id: UUID) -> list[DQMetricRow]:
    """
    Build table to table relation checks for the tables where they matter.
    """
    rows: list[DQMetricRow] = []

    if table_name == "stg_orders":
        missing_customers = _count_missing_customer_orders(conn, run_id=run_id)
        rows.append(
            _metric(
                run_id=run_id,
                table_name=table_name,
                check_name="stage_relations",
                metric_name="missing_customers.count",
                metric_value=missing_customers,
                passed=(missing_customers == 0),
                details_json={
                    "left_table": "stg_orders",
                    "right_table": "stg_customers",
                    "join_key": ["run_id", "customer_id"],
                    "missing_rows": missing_customers,
                },
            )
        )


    if table_name == "stg_order_items":
        missing_products = _count_missing_product_items(conn, run_id=run_id)
        orphan_orders = _count_orphan_order_items(conn, run_id=run_id)

        rows.append(
            _metric(
                run_id=run_id,
                table_name=table_name,
                check_name="stage_relations",
                metric_name="missing_products.count",
                metric_value=missing_products,
                passed=(missing_products == 0),
                details_json={
                    "left_table": "stg_order_items",
                    "right_table": "stg_products",
                    "join_key": ["run_id", "product_id"],
                    "missing_rows": missing_products,
                },
            )
        )


        rows.append(
            _metric(
                run_id=run_id,
                table_name=table_name,
                check_name="stage_relations",
                metric_name="orphan_orders.count",
                metric_value=orphan_orders,
                passed=(orphan_orders == 0),
                details_json={
                    "left_table": "stg_order_items",
                    "right_table": "stg_orders",
                    "join_key": ["run_id", "order_id"],
                    "missing_rows": orphan_orders,
                },
            )
        )

    return rows


def _build_metrics_for_table(conn: Connection, *, table_name: str, run_id: UUID) -> list[DQMetricRow]:
    """Build the full set of DQ metric rows for one staged table."""
    if table_name not in TABLE_SPECS:
        allowed = ", ".join(sorted(TABLE_SPECS))
        raise ValueError(f"unsupported table_name for DQ: {table_name!r}. Allowed: {allowed}")

    spec = TABLE_SPECS[table_name]

    rows: list[DQMetricRow] = []
    rows.extend(_build_volume_and_reject_metrics(conn, spec=spec, run_id=run_id))
    rows.extend(_build_relation_metrics(conn, table_name=table_name, run_id=run_id))
    return rows


def run_table_dq(conn: Connection, *, run_id: UUID, table_name: str) -> DQRunSummary:
    """
    Run DQ for one staged table and upsert rows into `dq_results`.
    """
    _ensure_run_exists(conn, run_id=run_id)

    metric_rows = _build_metrics_for_table(conn, table_name=table_name, run_id=run_id)

    # Idempotentcy per table, replaces previous metric set for that table.
    delete_dq_results(conn, run_id=run_id, table_name=table_name)
    inserted = upsert_dq_results(conn, rows=metric_rows)

    failed_metrics = sum(1 for row in metric_rows if not row.passed)

    return DQRunSummary(
        run_id=run_id,
        table_name=table_name,
        metrics_written=inserted,
        failed_metrics=failed_metrics,
        passed=(failed_metrics == 0),
    )



def run_stage_dq(conn: Connection, *, run_id: UUID) -> tuple[DQRunSummary, ...]:
    """
    Runs DQ across all known staged tables for one full pipeline run.

    Returns `summaries` in deterministic table order.
    """
    summaries: list[DQRunSummary] = []

    for table_name in TABLE_SPECS:
        summaries.append(run_table_dq(conn, run_id=run_id, table_name=table_name))

    return tuple(summaries)
