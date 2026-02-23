from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb



@dataclass(frozen=True)
class DQMetricRow:
    """Data quality information per row schema."""
    run_id: UUID
    table_name: str
    check_name: str             # which check this metric row comes from
    metric_name: str            # formal name for the analaysed metric
    metric_value: Decimal       # value of the metric
    passed: bool                # currently a hard > 0 failures means False
    details: Mapping[str, Any]


def delete_dq_results(conn: Connection, *, run_id: UUID, table_name: str) -> None:
    """Deletes a table given a `run_id` and a `table_name`."""
    conn.execute(
        "DELETE FROM dq_results WHERE run_id = %s AND table_name = %s",
        (run_id, table_name),
    )


def insert_dq_results(conn: Connection, *, rows: Iterable[DQMetricRow]) -> int:
    """
    Inserts metric rows into the `dq_results` table. Returns the inserted row count.
    """
    materialized = list(rows)
    if not materialized:        # on no rows, return 0
        return 0

    params = [
        (
            r.run_id,
            r.table_name,
            r.check_name,
            r.metric_name,
            r.metric_value,
            r.passed,
            Jsonb(dict(r.details)),
        )
        for r in materialized
    ]
    
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dq_results (
            run_id, table_name, check_name, metric_name, metric_value, passed, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            params,
        )
    return len(materialized)