from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.run_ledger import RunMode

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


def _get_run_mode(conn: Connection, *, run_id: UUID) -> RunMode:
    """Reads the pipeline mode from `run_ledger`."""
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
    if mode not in ("snapshot", "live"):
        raise ValueError(f"unsupported run mode for run_id={run_id}: {mode!r}")
    return mode


def _fetch_metric(conn: Connection, *, run_id: UUID, table_name: str, metric_name: str) -> Decimal:
    """
    Fetches one metric value from `dq_results`.

    Gating only runs after DQ has written its full metric set and is done
    """
    row = conn.execute(
        """
        SELECT metric_value
        FROM dq_results
        WHERE run_id = %s
          AND table_name = %s
          AND metric_name = %s
        """,
        (run_id, table_name, metric_name),
    ).fetchone()

    # Missing metric rows as this stage error.
    if row is None:
        raise ValueError(
            f"missing DQ metric for gating: run_id={run_id} "
            f"table_name={table_name!r} metric_name={metric_name!r}"
        )

    return Decimal(str(row[0]))


# failure types


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


def evaluate_stage_gates(conn: Connection, *, run_id: UUID) -> GateDecision:
    """
    Evaluate the overall pass or fail gates from DQ metrics.

    - all duplicate key counts must be 0
    - all relationship break counts must be 0
    - snapshot mode: reject rate must be exactly 0 for every stage table
    - live mode: reject rate may be non-zero, but must stay <= 1%
    """
    mode = _get_run_mode(conn, run_id=run_id)

    failures: list[GateFailure] = []
    warnings: list[GateFailure] = []

    stage_tables = (
        "stg_customers",
        "stg_products",
        "stg_orders",
        "stg_order_items",
    )

    # Basic table health
    for table_name in stage_tables:
        row_count = _fetch_metric(
            conn, run_id=run_id, table_name=table_name, metric_name="row_count"
        )
        duplicate_keys = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="duplicate_keys.count",
        )
        reject_rate = _fetch_metric(
            conn,
            run_id=run_id,
            table_name=table_name,
            metric_name="reject_rows.reject_rate",
        )

        if row_count == 0:
            warnings.append(
                _soft_warning(
                    table_name=table_name,
                    metric_name="row_count",
                    actual=row_count,
                    rule="expected > 0 rows",
                )
            )

        if duplicate_keys != 0:
            failures.append(
                _hard_failure(
                    table_name=table_name,
                    metric_name="duplicate_keys.count",
                    actual=duplicate_keys,
                    rule="must equal 0",
                )
            )

        # no rejections for snapshot mode.
        if mode == "snapshot":
            if reject_rate != 0:
                failures.append(
                    _hard_failure(
                        table_name=table_name,
                        metric_name="reject_rows.reject_rate",
                        actual=reject_rate,
                        rule="snapshot runs require reject_rows.reject_rate == 0",
                    )
                )

        # in live mode rejection is tolerated within tolerance
        else:
            live_threshold = Decimal("0.010000")
            live_warn_threshold = Decimal("0.005000")

            # hard
            if reject_rate > live_threshold:
                failures.append(
                    _hard_failure(
                        table_name=table_name,
                        metric_name="reject_rows.reject_rate",
                        actual=reject_rate,
                        rule="live runs require reject_rows.reject_rate <= 0.010000",
                    )
                )

            # soft
            elif reject_rate > live_warn_threshold:
                warnings.append(
                    _soft_warning(
                        table_name=table_name,
                        metric_name="reject_rows.reject_rate",
                        actual=reject_rate,
                        rule="warn when live reject_rows.reject_rate > 0.005000",
                    )
                )

    # table referential checks to other tables
    missing_customers = _fetch_metric(
        conn,
        run_id=run_id,
        table_name="stg_orders",
        metric_name="missing_customers.count",
    )
    if missing_customers != 0:
        failures.append(
            _hard_failure(
                table_name="stg_orders",
                metric_name="missing_customers.count",
                actual=missing_customers,
                rule="must equal 0",
            )
        )

    missing_products = _fetch_metric(
        conn,
        run_id=run_id,
        table_name="stg_order_items",
        metric_name="missing_products.count",
    )
    if missing_products != 0:
        failures.append(
            _hard_failure(
                table_name="stg_order_items",
                metric_name="missing_products.count",
                actual=missing_products,
                rule="must equal 0",
            )
        )

    orphan_orders = _fetch_metric(
        conn,
        run_id=run_id,
        table_name="stg_order_items",
        metric_name="orphan_orders.count",
    )

    if orphan_orders != 0:
        failures.append(
            _hard_failure(
                table_name="stg_order_items",
                metric_name="orphan_orders.count",
                actual=orphan_orders,
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
