from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class DQMetricRow:
    """Data quality information per row schema."""

    run_id: UUID
    table_name: str
    check_name: str  # which check this metric row comes from
    metric_name: str  # name of the analaysed metric
    metric_value: Decimal  # value of the metric
    passed: bool  # currently a hard > 0 failures means False
    details_json: Mapping[str, Any]


def delete_dq_results(conn: Connection, *, run_id: UUID, table_name: str) -> None:
    """Deletes previously written `dq_results` rows for given `run_id` and `table_name`."""
    conn.execute(
        "DELETE FROM dq_results WHERE run_id = %s AND table_name = %s",
        (run_id, table_name),
    )


def upsert_dq_results(conn: Connection, *, rows: Iterable[DQMetricRow]) -> int:
    """
    Inserts or updates metric rows into the `dq_results` table. Returns the inserted row count.
    """
    materialized = list(rows)
    if not materialized:  # on no rows, return 0
        return 0

    params = [
        (
            r.run_id,
            r.table_name,
            r.check_name,
            r.metric_name,
            r.metric_value,
            r.passed,
            Jsonb(dict(r.details_json)),
        )
        for r in materialized
    ]

    with conn.cursor() as cur:
        cur.executemany(  # on conflict, do upsert behaviour
            """
            INSERT INTO dq_results (
            run_id, table_name, check_name, metric_name, metric_value, passed, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, table_name, check_name, metric_name)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                passed = EXCLUDED.passed,
                details_json = EXCLUDED.details_json,
                created_at = now()
            """,
            params,
        )
    return len(materialized)
