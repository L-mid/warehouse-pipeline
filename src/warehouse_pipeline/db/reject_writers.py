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
    source_row: int
    raw_payload: Mapping[str, Any]
    reason_code: str
    reason_detail: str


# cols that should expect jsonb conversion
_JSONB_COLS = {"raw_payload"}



def _adapt(col: str, value: Any) -> Any:
    """Adapt python values to DB types (e.g., `jsonb`)."""
    if col in _JSONB_COLS and value is not None:
        return Jsonb(value)
    return value



def insert_reject_rows(conn: Connection, *, run_id: UUID, rejects: Sequence[RejectInsert]) -> None:
    """
    Insert `rejects` into the DB's `reject_rows`.
    
    Table/column identifiers are fixed/non derived constants. 
    Values are parameterized directly.
    """

    # fixed cols in `reject_rows`:
    cols = ("run_id", "table_name", "source_row", "raw_payload", "reason_code", "reason_detail")

    # paramaterize in values per col.
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
                r.source_row,
                _adapt("raw_payload", dict(r.raw_payload)),
                r.reason_code,
                r.reason_detail,
            )
        )
 
    if params:
        with conn.cursor() as cur:
            cur.executemany(query, params)  # sequential batch processing