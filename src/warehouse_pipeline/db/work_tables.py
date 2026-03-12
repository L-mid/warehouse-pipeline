from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from warehouse_pipeline.db.writers.staging import StagingTableSpec, get_staging_spec


@dataclass(frozen=True)
class WorkRow:
    """
    A staging ready row that is valid by all parsing, but not yet deduped.

    `raw_payload` and `source_row` are used to create `reject_rows` entries
    for rejecting duplicates discovered during the SQL finalization.
    """

    source_ref: int
    raw_payload: Mapping[str, Any]
    values: Mapping[str, Any]


def prepare_work_table(conn: Connection, *, table_name: str) -> None:
    """
    Create a temporary work table mirroring the target staging table.
    Also includes injected in `source_ref` and `raw_payload`.

    This temp table is scoped to the DB session only.
    """
    spec = get_staging_spec(table_name)
    work = sql.Identifier(spec.work_table_name)
    staging = sql.Identifier(spec.table_name)

    with conn.cursor() as cur:
        # Drop previous table and recreate every run so there's previous temp table.
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {work}").format(
                work=work,
            )
        )
        # has all the same cols as real
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {work} (LIKE {staging} INCLUDING DEFAULTS)").format(
                work=work,
                staging=staging,
            )
        )

        # These are required for duplicate rejection later.

        # add `raw_payload` for rejects.
        cur.execute(
            sql.SQL(
                "ALTER TABLE {work} ADD COLUMN IF NOT EXISTS raw_payload jsonb NOT NULL;"
            ).format(work=work)
        )

        # add `source_ref` for deterministic dup rejection logic later
        cur.execute(
            sql.SQL(
                "ALTER TABLE {work} ADD COLUMN IF NOT EXISTS source_ref integer NOT NULL;"
            ).format(work=work)
        )


def insert_work_rows(
    conn: Connection,
    *,
    table_name: str,
    run_id: UUID,
    rows: Sequence[WorkRow],
) -> int:
    """
    Inserts parsed rows accepted for staging into the work table.

    Never raises on duplicate business keys because the work table has no uniqueness constraints.
    Returns a count of its total params used.
    """
    if not rows:
        return 0

    spec = get_staging_spec(table_name)  # all table specs to be fetched from this wrap only
    # all cols only acceptable if derived from spec
    cols = ("run_id",) + spec.columns + ("source_ref", "raw_payload")

    # interpolating table and fields in now only after derivation from ok spec
    query = sql.SQL("INSERT INTO {work} ({cols}) VALUES ({vals})").format(
        work=sql.Identifier(spec.work_table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )

    # params:
    params: list[tuple[Any, ...]] = []
    for r in rows:
        values: list[Any] = [run_id]

        # staging the cols
        for c in spec.columns:
            values.append(r.values.get(c))

        # inject in `source_ref`
        values.append(r.source_ref)

        # `raw_payload` injection
        values.append(Jsonb(dict(r.raw_payload)))

        params.append(tuple(values))

    with conn.cursor() as cur:
        cur.executemany(query, params)

    return len(params)


def flush_work_table(conn: Connection, *, table_name: str, run_id: UUID) -> tuple[int, int]:
    """
    Deduplicate the work table into staging and emit duplicate rejects.

    Winner determination logic:
    - the first `source_ref` wins per business key
    - later duplicates are inserted into `reject_rows` as `duplicate_key` reason

    Returns:
    - (`inserted_count`, `duplicate_reject_count`)
    """
    spec = get_staging_spec(table_name)

    if isinstance(spec.key_cols, str):
        raise TypeError(
            f"{table_name}.key_cols must be tuple[str,...], got str={spec.key_cols!r}. "
        )
    if not spec.key_cols:
        # specs must possess a PK
        raise ValueError(f"table has no `key_cols` configured, {table_name}")

    staging_cols = ("run_id",) + spec.columns

    key_partition = sql.SQL(", ").join(sql.Identifier(c) for c in spec.key_cols)
    select_staging_cols = sql.SQL(", ").join(sql.Identifier(col) for col in staging_cols)
    insert_staging_cols = sql.SQL(", ").join(sql.Identifier(col) for col in staging_cols)

    # handles exact rejection behaviour if dup rejections are found
    duplicate_detail_expr = _duplicate_reason_detail_expr(spec)

    # select ranked winner by `source_ref` ASC, and inject in `source_ref` and rejects.
    query = sql.SQL(
        """
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY {key_partition}
                    ORDER BY source_ref ASC
                ) AS rn
            FROM {work}
            WHERE run_id = %s
        ),
        inserted AS (
            INSERT INTO {staging} ({insert_staging_cols})
            SELECT {select_staging_cols}
            FROM ranked
            WHERE rn = 1
            RETURNING 1
        ),
        duplicates AS (
            INSERT INTO reject_rows (
                run_id,
                table_name,
                source_ref,
                raw_payload,
                reason_code,
                reason_detail
            )
            SELECT
                run_id,
                %s,
                source_ref,
                raw_payload,
                'duplicate_key',
                {duplicate_detail_expr}
            FROM ranked
            WHERE rn > 1
            RETURNING 1
        )
        SELECT
            COALESCE((SELECT COUNT(*) FROM inserted), 0) AS inserted_count,
            COALESCE((SELECT COUNT(*) FROM duplicates), 0) AS duplicate_count
        """
    ).format(
        key_partition=key_partition,
        work=sql.Identifier(spec.work_table_name),
        staging=sql.Identifier(spec.table_name),
        insert_staging_cols=insert_staging_cols,
        select_staging_cols=select_staging_cols,
        duplicate_detail_expr=duplicate_detail_expr,
    )

    row = conn.execute(query, (run_id, table_name)).fetchone()
    assert row is not None  # empty row not allowed
    inserted_count, duplicate_count = row  # full row

    return int(inserted_count), int(duplicate_count)


def _duplicate_reason_detail_expr(spec: StagingTableSpec) -> sql.Composable:
    """
    Returns an SQL expression for `reject_rows.reason_detail`
    on duplicate keys for good injection.
    """
    parts: list[sql.Composable] = []
    # specially formatted return values in this case
    for c in spec.key_cols:
        parts.append(
            sql.SQL("concat({name}, '=', coalesce({col}::text, 'NULL'))").format(
                name=sql.Literal(c),
                col=sql.Identifier(c),
            )
        )

    # preformate parts to paramaterize in
    if len(parts) == 1:
        joined = parts[0]
    else:
        joined = sql.SQL("concat_ws(', ', {parts})").format(
            parts=sql.SQL(", ").join(parts),
        )

    return sql.SQL("concat('duplicate key: ', {joined})").format(joined=joined)
