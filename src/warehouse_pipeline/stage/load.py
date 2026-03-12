from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import cast
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.work_tables import (
    WorkRow,
    flush_work_table,
    insert_work_rows,
    prepare_work_table,
)
from warehouse_pipeline.db.writers.rejects import RejectInsert, insert_reject_rows
from warehouse_pipeline.stage import (
    MappedCarts,
    MappedProducts,
    MappedUsers,
    StageReject,
    StageRow,
    StageTableLoadResult,
)

# specified order in which to load tables.
_TABLE_LOAD_ORDER = (
    "stg_customers",
    "stg_products",
    "stg_orders",
    "stg_order_items",
)


def _as_work_rows(rows: Sequence[StageRow]) -> list[WorkRow]:
    """Valid work row."""
    return [
        WorkRow(
            source_ref=row.source_ref,
            raw_payload=row.raw_payload,
            values=row.values,
        )
        for row in rows
    ]


def _as_reject_inserts(rejects: Sequence[StageReject]) -> list[RejectInsert]:
    """Invaild reject row."""
    return [
        RejectInsert(
            table_name=reject.table_name,
            source_ref=reject.source_ref,
            raw_payload=reject.raw_payload,
            reason_code=reject.reason_code,
            reason_detail=reject.reason_detail,
        )
        for reject in rejects
    ]


def load_stage_rows(
    conn: Connection,
    *,
    run_id: UUID,
    rows: Iterable[StageRow],
    rejects: Iterable[StageReject] = (),
) -> dict[str, StageTableLoadResult]:
    """
    Load mapped stage rows into Postgres work tables and flush into `stg_*`.

    This function does not commit, transaction scope stays with the
    orchestration layer.
    """
    rows_by_table: dict[str, list[StageRow]] = defaultdict(list)
    reject_list = list(rejects)

    for row in rows:
        rows_by_table[row.table_name].append(row)

    explicit_reject_counts: dict[str, int] = defaultdict(int)
    for reject in reject_list:
        explicit_reject_counts[reject.table_name] += 1

    if reject_list:
        insert_reject_rows(conn, run_id=run_id, rejects=_as_reject_inserts(reject_list))

    results: dict[str, StageTableLoadResult] = {}
    for table_name in _TABLE_LOAD_ORDER:
        table_rows = rows_by_table.get(table_name, [])
        if not table_rows and explicit_reject_counts.get(table_name, 0) == 0:
            continue

        inserted_count = 0
        duplicate_reject_count = 0

        if table_rows:
            prepare_work_table(conn, table_name=table_name)
            insert_work_rows(
                conn, table_name=table_name, run_id=run_id, rows=_as_work_rows(table_rows)
            )
            inserted_count, duplicate_reject_count = cast(
                tuple[int, int],
                flush_work_table(conn, table_name=table_name, run_id=run_id),
            )

        results[table_name] = StageTableLoadResult(
            table_name=table_name,
            inserted_count=inserted_count,
            duplicate_reject_count=duplicate_reject_count,
            explicit_reject_count=explicit_reject_counts.get(table_name, 0),
        )

    return results


def load_mapped_batches(
    conn: Connection,
    *,
    run_id: UUID,
    users: MappedUsers,
    products: MappedProducts,
    carts: MappedCarts,
) -> dict[str, StageTableLoadResult]:
    """Convenience wrapper for loading the `DummyJSON` stage batches and `reject_rows`."""
    all_rows: list[StageRow] = [
        *users.rows,
        *products.rows,
        *carts.order_rows,
        *carts.order_item_rows,
    ]
    all_rejects: list[StageReject] = [
        *users.rejects,
        *products.rejects,
        *carts.rejects,
    ]
    return load_stage_rows(conn, run_id=run_id, rows=all_rows, rejects=all_rejects)
