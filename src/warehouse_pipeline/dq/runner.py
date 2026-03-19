from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from uuid import UUID

from psycopg import Connection, sql

from warehouse_pipeline.db.dq_results import DQMetricRow, delete_dq_results, upsert_dq_results
from warehouse_pipeline.db.writers.staging import TABLE_SPECS, StagingTableSpec
from warehouse_pipeline.dq.thresholds import VOLUME_BASELINE_LOOKBACK

_Q6 = Decimal("0.000000")  # this one is align with numeric(18,6), for higher precison.

_STAGE_TABLES = (
    "stg_square_orders",
    "stg_square_order_lines",
    "stg_square_tenders",
)

_FACT_TABLES = (
    "fact_orders",
    "fact_order_lines",
    "fact_order_tenders",
)

_FACT_TO_STAGE = {
    "fact_orders": "stg_square_orders",
    "fact_order_lines": "stg_square_order_lines",
    "fact_order_tenders": "stg_square_tenders",
}


@dataclass(frozen=True)
class DQRunSummary:
    """
    Summary of a run's DQ execution.
    """

    run_id: UUID
    table_name: str
    metrics_written: int
    failed_metrics: int
    passed: bool


@dataclass(frozen=True)
class RunInfo:
    run_id: UUID
    mode: str
    source_system: str
    started_at: datetime


def _q6(value: Decimal | int | str | float) -> Decimal:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(_Q6, rounding=ROUND_HALF_UP)


def _metric(
    *,
    run_id: UUID,
    table_name: str,
    check_name: str,
    metric_name: str,
    metric_value: Decimal | int | str | float,
    passed: bool,
    details_json: dict[str, Any],
) -> DQMetricRow:
    return DQMetricRow(
        run_id=run_id,
        table_name=table_name,
        check_name=check_name,
        metric_name=metric_name,
        metric_value=_q6(metric_value),
        passed=passed,
        details_json=details_json,
    )


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


def _get_run_info(conn: Connection, *, run_id: UUID) -> RunInfo:
    row = conn.execute(
        """
        SELECT run_id, mode, source_system, started_at
        FROM run_ledger
        WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"run_id not found in run_ledger: {run_id}")

    return RunInfo(
        run_id=row[0],
        mode=str(row[1]),
        source_system=str(row[2]),
        started_at=row[3],
    )


def _count_stage_rows(conn: Connection, *, table_name: str, run_id: UUID) -> int:
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


def _count_fact_rows(conn: Connection, *, table_name: str, run_id: UUID) -> int:
    q = sql.SQL(
        """
        SELECT COUNT(*)
        FROM {table_name}
        WHERE source_run_id = %s
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


def _max_orders_updated_at(conn: Connection, *, run_id: UUID) -> datetime | None:
    row = conn.execute(
        """
        SELECT MAX(updated_at_source)
        FROM stg_square_orders
        WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def _count_orphan_order_lines(conn: Connection, *, run_id: UUID) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_square_order_lines l
        LEFT JOIN stg_square_orders o
          ON o.run_id = l.run_id
         AND o.order_id = l.order_id
        WHERE l.run_id = %s
          AND o.order_id IS NULL
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _count_orphan_tenders(conn: Connection, *, run_id: UUID) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_square_tenders t
        LEFT JOIN stg_square_orders o
          ON o.run_id = t.run_id
         AND o.order_id = t.order_id
        WHERE t.run_id = %s
          AND o.order_id IS NULL
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _prior_metric_values(
    conn: Connection,
    *,
    current_run_id: UUID,
    source_system: str,
    table_name: str,
    metric_name: str,
    lookback: int = VOLUME_BASELINE_LOOKBACK,
) -> list[Decimal]:
    rows = conn.execute(
        """
        SELECT dr.metric_value
        FROM dq_results dr
        JOIN run_ledger rl
          ON rl.run_id = dr.run_id
        WHERE rl.status = 'succeeded'
          AND rl.source_system = %s
          AND rl.mode IN ('live', 'incremental')
          AND dr.table_name = %s
          AND dr.metric_name = %s
          AND dr.run_id <> %s
        ORDER BY rl.finished_at DESC NULLS LAST, rl.started_at DESC
        LIMIT %s
        """,
        (source_system, table_name, metric_name, current_run_id, lookback),
    ).fetchall()

    return [Decimal(str(row[0])) for row in rows]


def _median_decimal(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")

    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2

    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def _build_stage_volume_and_reject_metrics(
    conn: Connection,
    *,
    run_info: RunInfo,
    spec: StagingTableSpec,
) -> list[DQMetricRow]:
    table_name = spec.table_name
    row_count = _count_stage_rows(conn, table_name=table_name, run_id=run_info.run_id)
    duplicate_keys = _count_duplicate_keys(conn, spec=spec, run_id=run_info.run_id)
    reject_rows_by_reason = _reject_counts_by_reason(
        conn,
        run_id=run_info.run_id,
        table_name=table_name,
    )
    total_rejects = sum(n for _, n in reject_rows_by_reason)

    reject_rate = Decimal("0")
    if row_count > 0:
        reject_rate = Decimal(total_rejects) / Decimal(row_count)

    prior_row_counts = _prior_metric_values(
        conn,
        current_run_id=run_info.run_id,
        source_system=run_info.source_system,
        table_name=table_name,
        metric_name="row_count",
    )
    baseline_median = _median_decimal(prior_row_counts)
    baseline_sample_size = len(prior_row_counts)

    ratio_to_baseline = Decimal("0")
    if baseline_median > 0:
        ratio_to_baseline = Decimal(row_count) / baseline_median

    rows: list[DQMetricRow] = [
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_volume",
            metric_name="row_count",
            metric_value=row_count,
            passed=(row_count > 0),
            details_json={"row_count": row_count},
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_keys",
            metric_name="duplicate_keys.count",
            metric_value=duplicate_keys,
            passed=(duplicate_keys == 0),
            details_json={
                "key_cols": list(spec.key_cols),
                "duplicate_key_groups": duplicate_keys,
            },
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_rejects",
            metric_name="reject_rows.total",
            metric_value=total_rejects,
            passed=(total_rejects == 0),
            details_json={
                "row_count": row_count,
                "total_rejects": total_rejects,
            },
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_rejects",
            metric_name="reject_rows.reject_rate",
            metric_value=reject_rate,
            passed=(reject_rate == 0),
            details_json={
                "row_count": row_count,
                "total_rejects": total_rejects,
                "formula": "total_rejects / row_count",
            },
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_volume_baseline",
            metric_name="volume.sample_size",
            metric_value=baseline_sample_size,
            passed=True,
            details_json={"sample_size": baseline_sample_size},
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_volume_baseline",
            metric_name="volume.row_count_baseline_median",
            metric_value=baseline_median,
            passed=True,
            details_json={
                "sample_size": baseline_sample_size,
                "prior_row_counts": [str(v) for v in prior_row_counts],
            },
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=table_name,
            check_name="stage_volume_baseline",
            metric_name="volume.row_count_ratio_to_baseline",
            metric_value=ratio_to_baseline,
            passed=True,
            details_json={
                "row_count": row_count,
                "baseline_median": str(baseline_median),
                "sample_size": baseline_sample_size,
            },
        ),
    ]

    for reason_code, count in reject_rows_by_reason:
        rows.append(
            _metric(
                run_id=run_info.run_id,
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


def _build_relation_metrics(
    conn: Connection,
    *,
    run_info: RunInfo,
    table_name: str,
) -> list[DQMetricRow]:
    rows: list[DQMetricRow] = []

    if table_name == "stg_square_order_lines":
        orphan_orders = _count_orphan_order_lines(conn, run_id=run_info.run_id)
        rows.append(
            _metric(
                run_id=run_info.run_id,
                table_name=table_name,
                check_name="stage_relations",
                metric_name="orphan_orders.count",
                metric_value=orphan_orders,
                passed=(orphan_orders == 0),
                details_json={
                    "child_table": "stg_square_order_lines",
                    "parent_table": "stg_square_orders",
                    "join_key": ["run_id", "order_id"],
                    "missing_rows": orphan_orders,
                },
            )
        )

    if table_name == "stg_square_tenders":
        orphan_orders = _count_orphan_tenders(conn, run_id=run_info.run_id)
        rows.append(
            _metric(
                run_id=run_info.run_id,
                table_name=table_name,
                check_name="stage_relations",
                metric_name="orphan_orders.count",
                metric_value=orphan_orders,
                passed=(orphan_orders == 0),
                details_json={
                    "child_table": "stg_square_tenders",
                    "parent_table": "stg_square_orders",
                    "join_key": ["run_id", "order_id"],
                    "missing_rows": orphan_orders,
                },
            )
        )

    return rows


def _build_orders_freshness_metrics(
    conn: Connection,
    *,
    run_info: RunInfo,
) -> list[DQMetricRow]:
    max_updated_at = _max_orders_updated_at(conn, run_id=run_info.run_id)

    age_hours = Decimal("0")
    has_timestamp = max_updated_at is not None
    if max_updated_at is not None:
        age_hours = Decimal(str((run_info.started_at - max_updated_at).total_seconds())) / Decimal(
            "3600"
        )

    return [
        _metric(
            run_id=run_info.run_id,
            table_name="stg_square_orders",
            check_name="freshness",
            metric_name="freshness.max_updated_at_age_hours",
            metric_value=age_hours,
            passed=has_timestamp,
            details_json={
                "run_started_at": run_info.started_at.isoformat(),
                "max_updated_at_source": max_updated_at.isoformat() if max_updated_at else None,
                "has_timestamp": has_timestamp,
                "age_hours": str(age_hours),
            },
        )
    ]


def _build_fact_metrics(
    conn: Connection,
    *,
    run_info: RunInfo,
    fact_table_name: str,
) -> list[DQMetricRow]:
    stage_table_name = _FACT_TO_STAGE[fact_table_name]
    stage_row_count = _count_stage_rows(conn, table_name=stage_table_name, run_id=run_info.run_id)
    fact_row_count = _count_fact_rows(conn, table_name=fact_table_name, run_id=run_info.run_id)
    count_diff = abs(fact_row_count - stage_row_count)

    return [
        _metric(
            run_id=run_info.run_id,
            table_name=fact_table_name,
            check_name="warehouse_parity",
            metric_name="warehouse_parity.stage_row_count",
            metric_value=stage_row_count,
            passed=True,
            details_json={"stage_table_name": stage_table_name},
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=fact_table_name,
            check_name="warehouse_parity",
            metric_name="warehouse_parity.fact_row_count",
            metric_value=fact_row_count,
            passed=True,
            details_json={"fact_table_name": fact_table_name},
        ),
        _metric(
            run_id=run_info.run_id,
            table_name=fact_table_name,
            check_name="warehouse_parity",
            metric_name="warehouse_parity.count_diff",
            metric_value=count_diff,
            passed=(count_diff == 0),
            details_json={
                "stage_table_name": stage_table_name,
                "stage_row_count": stage_row_count,
                "fact_row_count": fact_row_count,
            },
        ),
    ]


def _build_metrics_for_table(
    conn: Connection,
    *,
    run_info: RunInfo,
    table_name: str,
) -> list[DQMetricRow]:
    rows: list[DQMetricRow] = []

    if table_name in _STAGE_TABLES:
        spec = TABLE_SPECS[table_name]
        rows.extend(_build_stage_volume_and_reject_metrics(conn, run_info=run_info, spec=spec))
        rows.extend(_build_relation_metrics(conn, run_info=run_info, table_name=table_name))
        if table_name == "stg_square_orders":
            rows.extend(_build_orders_freshness_metrics(conn, run_info=run_info))
        return rows

    if table_name in _FACT_TABLES:
        rows.extend(_build_fact_metrics(conn, run_info=run_info, fact_table_name=table_name))
        return rows

    allowed = ", ".join(sorted((*_STAGE_TABLES, *_FACT_TABLES)))
    raise ValueError(f"unsupported table_name for DQ: {table_name!r}. Allowed: {allowed}")


def run_table_dq(conn: Connection, *, run_id: UUID, table_name: str) -> DQRunSummary:
    _ensure_run_exists(conn, run_id=run_id)
    run_info = _get_run_info(conn, run_id=run_id)

    metric_rows = _build_metrics_for_table(conn, run_info=run_info, table_name=table_name)

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


def run_model_dq(conn: Connection, *, run_id: UUID) -> tuple[DQRunSummary, ...]:
    summaries: list[DQRunSummary] = []

    for table_name in (*_STAGE_TABLES, *_FACT_TABLES):
        summaries.append(run_table_dq(conn, run_id=run_id, table_name=table_name))

    return tuple(summaries)
