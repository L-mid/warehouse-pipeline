from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from warehouse_pipeline.db.staging_writers import TABLE_SPECS, TableWriteSpec, adapt_staging_value



@dataclass(frozen=True)
class WorkRow:
    """
    A staging ready row that is valid by all parsing, but not yet deduped.

    `raw_payload` and `source_row` are used to create `reject_rows` entries
    for rejecting duplicates discovered during the SQL finalization.
    """

    source_row: int
    raw_payload: Mapping[str, Any]
    staging_mapping: Mapping[str, Any]



def prepare_work_table(conn: Connection, *, table_name: str) -> None:
    """
    Create a work table (if needed) and truncate the temp/work table for the given staging table.

    Uses a TEMP table which is scoped to the DB session/connection.
    `LIKE <staging>` used so column types can stay aligned with the canonical schema.
    
    Injects in `raw_payload jsonb` (always) and `source_row integer` (if missing) to support
    duplicate rejection with exepected precise lineage.

    Table identifiers are derived ONLY from the whitelisted spec `TABLE_SPECS`.
    """    
    spec = TABLE_SPECS[table_name]
    work = sql.Identifier(spec.work_table_name)
    staging = sql.Identifier(spec.table_name)

    with conn.cursor() as cur:
        # Recreate every run so there's no stale temp table.
        # creates the temp table with the same column types as staging. 
        # it is without PK/unique constraints.
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {work} (LIKE {staging} INCLUDING DEFAULTS);").format(
                work=work,
                staging=staging,
            )
        )

        # These are required for duplicate rejection later.
        
        # store `raw_payload` for rejects.
        cur.execute(
            sql.SQL("ALTER TABLE {work} ADD COLUMN IF NOT EXISTS raw_payload jsonb NOT NULL;").format(
                work=work
            )
        )

        # store `source_row` for deterministic winner-reject lineage logic later
        cur.execute(
            sql.SQL("ALTER TABLE {work} ADD COLUMN IF NOT EXISTS source_row integer NOT NULL;").format(
                work=work
            )
        )




def insert_work_rows(
    conn: Connection,
    *,
    table_name: str,
    run_id: UUID,
    rows: Sequence[WorkRow],
) -> None:
    """
    Insert parsed rows accepted for staging into the temp/work table.

    This should never raises on duplicate business keys because the work table has no uniqueness constraints.
    """
    if not rows:
        return

    spec = TABLE_SPECS[table_name]          # all table specs to be fetched from this wrap only
    cols = _work_insert_columns(spec)       # all cols only acceptable if derived from spec


    # interpolating table and fields in now only after derivation
    query = sql.SQL("INSERT INTO {work} ({cols}) VALUES ({vals})").format(
        work=sql.Identifier(spec.work_table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )

    # params:
    params: list[tuple[Any, ...]] = []
    for r in rows:
        tup: list[Any] = [run_id]

        # staging the cols 
        for c in spec.columns:
            tup.append(adapt_staging_value(c, r.staging_mapping.get(c)))

        # inject in `source_row` (IF it is not already a staging column)
        if "source_row" not in spec.columns:
            tup.append(int(r.source_row))

        # `raw_payload` injection
        tup.append(Jsonb(dict(r.raw_payload)))

        params.append(tuple(tup))

    with conn.cursor() as cur:
        cur.executemany(query, params)



    
def finalize_work_to_staging(conn: Connection, *, table_name: str, run_id: UUID) -> tuple[int, int]:
    """
    Deduplicate the work table into staging and emit duplicate rejects.

    Winner determination logic:
    - As "first seen wins" within the file. Logic is implemented as smallest `source_row` was 'first' and therefore 'wins'.

    Returns:
        (`inserted_to_staging`, `duplicate_rejects_inserted`)

    Why a set-based dedupe method:
    - avoiding rasing and swallowing on per-row DB exceptions 
    - deterministic and applies well to a scaled row batching stratagy
    """
    spec = TABLE_SPECS[table_name]

    if isinstance(spec.key_cols, str):
        raise TypeError(
            f"{table_name}.key_cols must be tuple[str,...], got str={spec.key_cols!r}. "
            f"Common bug: missing comma -> ('order_id' 'line_id') concatenates into single `str`."
        )
    if not spec.key_cols:
        # specs must possess a PK
        raise ValueError(f"table has no `key_cols` configured: {table_name}")

    key_partition = sql.SQL(", ").join(sql.Identifier(c) for c in spec.key_cols)
    staging_cols = ("run_id",) + spec.columns

    insert_staging = sql.SQL(
        "INSERT INTO {stg} ({cols})\n"
        "SELECT {cols}\n"
        "FROM ranked\n"
        "WHERE rn = 1\n"
        "RETURNING 1"
    ).format(
        stg=sql.Identifier(spec.table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in staging_cols),
    )

    # handles exact rejection behaviour if dup rejections are found
    dup_reason_detail_expr = _duplicate_reason_detail_expr(spec)

    insert_dups = sql.SQL(
        "INSERT INTO reject_rows (run_id, table_name, source_row, raw_payload, reason_code, reason_detail)\n"
        "SELECT run_id, %s, source_row, raw_payload, 'duplicate_key', {detail}\n"
        "FROM ranked\n"
        "WHERE rn > 1\n"
        "RETURNING 1"
    ).format(detail=dup_reason_detail_expr)

    q = sql.SQL(
        "WITH ranked AS (\n"
        "  SELECT *,\n"
        "    row_number() OVER (PARTITION BY {k} ORDER BY source_row ASC) AS rn\n"
        "  FROM {work}\n"
        "  WHERE run_id = %s\n"
        "),\n"
        "ins AS (\n"
        "  {insert_staging}\n"
        "),\n"
        "dups AS (\n"
        "  {insert_dups}\n"
        ")\n"
        "SELECT\n"
        "  (SELECT COUNT(*) FROM ins) AS inserted,\n"
        "  (SELECT COUNT(*) FROM dups) AS duplicates;\n"
    ).format(
        k=key_partition,
        work=sql.Identifier(spec.work_table_name),
        insert_staging=insert_staging,
        insert_dups=insert_dups,
    )

    row = conn.execute(q, (run_id, table_name)).fetchone()
    if row is None:
        return (0, 0)               # empty row
    inserted, duplicates = row      # full row
    return (int(inserted), int(duplicates))


def _work_insert_columns(spec: TableWriteSpec) -> tuple[str, ...]:
    """Deterministic Column order used for work-table inserts."""
    cols: list[str] = ["run_id", *spec.columns]
    if "source_row" not in spec.columns:
        cols.append("source_row")
    cols.append("raw_payload")
    return tuple(cols)


def _duplicate_reason_detail_expr(spec: TableWriteSpec) -> sql.Composable:
    """Build a safe SQL expression for `reject_rows.reason_detail` on duplicate keys."""
    parts: list[sql.Composable] = []
    # specially formatted return values in this case
    for c in spec.key_cols:
        parts.append(
            sql.SQL("concat({name}, '=', coalesce({col}::text, 'NULL'))").format(
                name=sql.Literal(c),
                col=sql.Identifier(c),
            )
        )

    return sql.SQL("concat('duplicate key on ', concat_ws(', ', {parts}))").format(
        parts=sql.SQL(", ").join(parts)
    )


