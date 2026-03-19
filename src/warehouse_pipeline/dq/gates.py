from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.run_ledger import RunMode
from warehouse_pipeline.dq.thresholds import (
    FRESHNESS_HARD_HOURS,
    FRESHNESS_WARN_HOURS,
    VOLUME_BASELINE_MIN_RUNS,
    VOLUME_RATIO_HARD_HIGH,
    VOLUME_RATIO_HARD_LOW,
    VOLUME_RATIO_WARN_HIGH,
    VOLUME_RATIO_WARN_LOW,
)

GateSeverity = Literal["hard", "soft"]


@dataclass(frozen=True)
class GateFailure:
    """
    One failed or warning gate evaluation.
    """

    table_name: str
    metric_name: str
    actual: Decimal
    rule: str
    severity: GateSeverity


@dataclass(frozen=True)
class GateDecision:
    """
    Stores final pass or fail decision for a pipeline run.
    """

    run_id: UUID
    mode: RunMode
    passed: bool
    failures: tuple[GateFailure, ...]
    warnings: tuple[GateFailure, ...]


@dataclass(frozen=True)
class MetricRow:
    metric_value: Decimal
    passed: bool
    details_json: dict[str, Any]


def _get_run_mode(conn: Connection, *, run_id: UUID) -> RunMode:
    row = conn.execute(
        """
        SELECT mode
        FROM run_ledger
        WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"run_id not found in run_ledger: {run_id}")

    mode = row[0]
    if mode not in ("snapshot", "live", "incremental"):
        raise ValueError(f"unsupported run mode for run_id={run_id}: {mode!r}")
    return mode


def _fetch_metric(
    conn: Connection, *, run_id: UUID, table_name: str, metric_name: str
) -> MetricRow:
    row = conn.execute(
        """
        SELECT metric_value, passed, details_json
        FROM dq_results
        WHERE run_id = %s
          AND table_name = %s
          AND metric_name = %s
        """,
        (run_id, table_name, metric_name),
    ).fetchone()

    if row is None:
        raise ValueError(
            f"missing DQ metric for gating: run_id={run_id} "
            f"table_name={table_name!r} metric_name={metric_name!r}"
        )

    return MetricRow(
        metric_value=Decimal(str(row[0])),
        passed=bool(row[1]),
        details_json=dict(row[2] or {}),
    )


def _try_fetch_metric(
    conn: Connection,
    *,
    run_id: UUID,
    table_name: str,
    metric_name: str,
) -> MetricRow | None:
    row = conn.execute(
        """
        SELECT metric_value, passed, details_json
        FROM dq_results
        WHERE run_id = %s
          AND table_name = %s
          AND metric_name = %s
        """,
        (run_id, table_name, metric_name),
    ).fetchone()

    if row is None:
        return None

    return MetricRow(
        metric_value=Decimal(str(row[0])),
        passed=bool(row[1]),
        details_json=dict(row[2] or {}),
    )


def _hard_failure(
    *,
    table_name: str,
    metric_name: str,
    actual: Decimal,
    rule: str,
) -> GateFailure:
    """Return `"hard"` gate."""
    return GateFailure(
        table_name=table_name,
        metric_name=metric_name,
        actual=actual,
        rule=rule,
        severity="hard",
    )


def _soft_warning(
    *,
    table_name: str,
    metric_name: str,
    actual: Decimal,
    rule: str,
) -> GateFailure:
    """Return `"soft"` gate."""
    return GateFailure(
        table_name=table_name,
        metric_name=metric_name,
        actual=actual,
        rule=rule,
        severity="soft",
    )


def evaluate_model_gates(conn: Connection, *, run_id: UUID) -> GateDecision:
    mode = _get_run_mode(conn, run_id=run_id)

    failures: list[GateFailure] = []
    warnings: list[GateFailure] = []

    stage_tables = (
        "stg_square_orders",
        "stg_square_order_lines",
        "stg_square_tenders",
    )

    fact_tables = (
        "fact_orders",
        "fact_order_lines",
        "fact_order_tenders",
    )

    # Basic row-count visibility.
    for table_name in (*stage_tables, *fact_tables):
        row_count_metric_name = (
            "row_count" if table_name.startswith("stg_") else "warehouse_parity.fact_row_count"
        )
        row_count = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name=row_count_metric_name,
        ).metric_value

        if row_count == 0:
            warnings.append(
                _soft_warning(
                    table_name=table_name,
                    metric_name=row_count_metric_name,
                    actual=row_count,
                    rule="expected > 0 rows",
                )
            )

    # Duplicate keys are always hard failures on stage.
    for table_name in stage_tables:
        duplicate_keys = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="duplicate_keys.count",
        ).metric_value
        if duplicate_keys != 0:
            failures.append(
                _hard_failure(
                    table_name=table_name,
                    metric_name="duplicate_keys.count",
                    actual=duplicate_keys,
                    rule="must equal 0",
                )
            )
    # Freshness only makes sense for live/incremental.
    if mode in ("live", "incremental"):
        freshness = _fetch_metric(
            conn,
            run_id=run_id,
            table_name="stg_square_orders",
            metric_name="freshness.max_updated_at_age_hours",
        ).metric_value

        if freshness > FRESHNESS_HARD_HOURS:
            failures.append(
                _hard_failure(
                    table_name="stg_square_orders",
                    metric_name="freshness.max_updated_at_age_hours",
                    actual=freshness,
                    rule=f"must be <= {FRESHNESS_HARD_HOURS} hours",
                )
            )
        elif freshness > FRESHNESS_WARN_HOURS:
            warnings.append(
                _soft_warning(
                    table_name="stg_square_orders",
                    metric_name="freshness.max_updated_at_age_hours",
                    actual=freshness,
                    rule=f"warn when > {FRESHNESS_WARN_HOURS} hours",
                )
            )

        # Volume gates only when enough baseline history exists.
        for table_name in stage_tables:
            sample_size = _fetch_metric(
                conn,
                run_id=run_id,
                table_name=table_name,
                metric_name="volume.sample_size",
            ).metric_value
            baseline_median = _fetch_metric(
                conn,
                run_id=run_id,
                table_name=table_name,
                metric_name="volume.row_count_baseline_median",
            ).metric_value
            ratio = _fetch_metric(
                conn,
                run_id=run_id,
                table_name=table_name,
                metric_name="volume.row_count_ratio_to_baseline",
            ).metric_value

            if sample_size < VOLUME_BASELINE_MIN_RUNS or baseline_median <= 0:
                continue

            if ratio < VOLUME_RATIO_HARD_LOW or ratio > VOLUME_RATIO_HARD_HIGH:
                failures.append(
                    _hard_failure(
                        table_name=table_name,
                        metric_name="volume.row_count_ratio_to_baseline",
                        actual=ratio,
                        rule=(
                            f"must stay within "
                            f"[{VOLUME_RATIO_HARD_LOW}, {VOLUME_RATIO_HARD_HIGH}] "
                            f"once baseline exists"
                        ),
                    )
                )
            elif ratio < VOLUME_RATIO_WARN_LOW or ratio > VOLUME_RATIO_WARN_HIGH:
                warnings.append(
                    _soft_warning(
                        table_name=table_name,
                        metric_name="volume.row_count_ratio_to_baseline",
                        actual=ratio,
                        rule=(f"warn outside [{VOLUME_RATIO_WARN_LOW}, {VOLUME_RATIO_WARN_HIGH}]"),
                    )
                )

    # Referential integrity: child rows must have a parent order.
    for table_name in ("stg_square_order_lines", "stg_square_tenders"):
        orphan_orders = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="orphan_orders.count",
        ).metric_value
        if orphan_orders != 0:
            failures.append(
                _hard_failure(
                    table_name=table_name,
                    metric_name="orphan_orders.count",
                    actual=orphan_orders,
                    rule="must equal 0",
                )
            )

    # Warehouse parity: fact counts for this source_run_id must match stage counts.
    for table_name in fact_tables:
        count_diff = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="warehouse_parity.count_diff",
        ).metric_value
        if count_diff != 0:
            failures.append(
                _hard_failure(
                    table_name=table_name,
                    metric_name="warehouse_parity.count_diff",
                    actual=count_diff,
                    rule="must equal 0",
                )
            )

    return GateDecision(
        run_id=run_id,
        mode=mode,
        passed=(len(failures) == 0),
        failures=tuple(failures),
        warnings=tuple(warnings),
    )


def render_dq_summary(conn: Connection, *, run_id: UUID, decision: GateDecision) -> str:
    lines: list[str] = []

    headline = "DQ PASS" if decision.passed else "DQ FAIL"
    lines.append(
        f"{headline}: {len(decision.failures)} hard failure(s), {len(decision.warnings)} warning(s)"
    )

    freshness = _try_fetch_metric(
        conn,
        run_id=run_id,
        table_name="stg_square_orders",
        metric_name="freshness.max_updated_at_age_hours",
    )
    if freshness is not None:
        lines.append(
            f"orders freshness: {freshness.metric_value.normalize()}h since latest updated_at"
        )

    for table_name in (
        "stg_square_orders",
        "stg_square_order_lines",
        "stg_square_tenders",
    ):
        ratio = _try_fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="volume.row_count_ratio_to_baseline",
        )
        sample_size = _try_fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="volume.sample_size",
        )
        if ratio is not None and sample_size is not None and sample_size.metric_value > 0:
            lines.append(
                f"{table_name} volume ratio: {ratio.metric_value.normalize()} "
                f"(baseline n={int(sample_size.metric_value)})"
            )

    for table_name in ("stg_square_order_lines", "stg_square_tenders"):
        orphan_metric = _try_fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="orphan_orders.count",
        )
        if orphan_metric is not None:
            lines.append(f"{table_name} orphan orders: {int(orphan_metric.metric_value)}")

    for table_name in ("fact_orders", "fact_order_lines", "fact_order_tenders"):
        parity_metric = _try_fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="warehouse_parity.count_diff",
        )
        if parity_metric is not None:
            lines.append(f"{table_name} parity diff: {int(parity_metric.metric_value)}")

    if decision.failures:
        lines.append("hard failures:")
        for failure in decision.failures:
            lines.append(
                f"  - {failure.table_name}.{failure.metric_name}={failure.actual} ({failure.rule})"
            )

    if decision.warnings:
        lines.append("warnings:")
        for warning in decision.warnings:
            lines.append(
                f"  - {warning.table_name}.{warning.metric_name}={warning.actual} ({warning.rule})"
            )

    return "\n".join(lines)
