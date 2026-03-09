from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from psycopg import Connection, sql
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class RejectInsert:
    """`reject_rows` table's expected data schema for inserting rejected rows."""
    table_name: str             
    source_ref: int
    raw_payload: Mapping[str, Any]
    reason_code: str
    reason_detail: str



def insert_reject_rows(conn: Connection, *, run_id: UUID, rejects: Sequence[RejectInsert]) -> None:
    """
    Insert `rejects` into the DB's `reject_rows`.
    
    Table and column identifiers are fixed derived constants. 
    Values are parameterized directly.
    """
    if not rejects:
        return 0

    # fixed cols in `reject_rows`:
    cols = ("run_id", "table_name", "source_ref", "raw_payload", "reason_code", "reason_detail")

    # paramaterize in the values per col.
    query = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({vals})").format(
        tbl=sql.Identifier("reject_rows"),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),   # does not include user-provided fields, safe to inject
    )

    # params:
    params: list[tuple[Any, ...]] = []
    for r in rejects:
        params.append(
            (
                run_id,
                r.table_name,
                r.source_ref,
                Jsonb(dict(r.raw_payload)),
                r.reason_code,
                r.reason_detail,
            )
        )
  
    if params:
        with conn.cursor() as cur:
            cur.executemany(query, params)  # sequential batch processing


    return len(params)