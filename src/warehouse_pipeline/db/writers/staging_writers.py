from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from psycopg import Connection, sql
from psycopg.types.json import Jsonb



@dataclass(frozen=True)
class TableWriteSpec:
    """Whitelisted staging-table contract used for safe SQL generation.

    Notes:
    - `columns` excludes `run_id` and `created_at` (`run_id` is injected, `created_at` has a DB default).
    - `key_cols` are the per-run key columns (excluding `run_id`). They match this staging PK shape:
    `PRIMARY KEY (run_id, *key_cols)`.
    """
    table_name: str
    columns: tuple[str, ...]
    key_cols: tuple[str, ...]

    @property
    def work_table_name(self) -> str:
        # here, temp/work table name is derived from the whitelisted staging `table_name` only. 
        return f"_work_{self.table_name}"



# wrap all staging specs for data together.
TABLE_SPECS: dict[str, TableWriteSpec] = {
    # `customers-1000.csv`.
    "stg_customers": TableWriteSpec(
        table_name="stg_customers",
        columns=(
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "company",
            "city",
            "country",
            "phone_1",
            "phone_2",
            "email",
            "subscription_date",
            "website",
        ),
        key_cols=("customer_id",),
    ),
    # `retail_transactions.csv`
    "stg_retail_transactions": TableWriteSpec(
        table_name="stg_retail_transactions",
        columns=(
            "source_row",
            "date",
            "week",
            "sku",
            "product_category",
            "gender",
            "marketplace",
            "fulfillment",
            "color",
            "size",
            "list_price",
            "discount_pct",
            "promo_type",
            "ad_spend",
            "impressions",
            "clicks",
            "cvr",
            "units_sold",
            "revenue",
            "rating",
            "reviews",
            "competitor_price_index",
            "stock_on_hand",
            "stockout_flag",
            "holiday_flag",
        ),
        key_cols=("source_row",),
    ),
    # `orders.csv`
    "stg_orders": TableWriteSpec(
        table_name="stg_orders",
        columns=(
            "order_id",
            "customer_id",
            "order_ts",
            "country",
            "status",
            "total_usd",
        ),
        key_cols=("order_id",),
    ), 
    # `order_items.csv`
    "stg_order_items": TableWriteSpec(
        table_name="stg_order_items",
        columns=(
            "order_id",
            "line_id",
            "sku",
            "qty",
            "unit_price_usd",
            "discount_usd",
        ),
        key_cols=("order_id", "line_id"),
    ),
}

# cols that should expect jsonb conversion
_JSONB_COLS = {"items"}


def adapt_staging_value(col: str, value: Any) -> Any:
    """Adapt python values to DB types (e.g., `jsonb`)."""
    if col in _JSONB_COLS and value is not None:
        return Jsonb(value)
    return value



def insert_staging_rows(
    conn: Connection,
    *,
    table_name: str,
    run_id: UUID,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """
    Inserts parsed rows into given a staging table. 
    
    Legacy: Used by simpler flows.
    Any given `rows` will then be inserted into the appropriate staging table.

    Parsed mapping keys should already match staging column names at this step.
    Does not interpolate or SQL-inject user-provided values directly.
    - identifiers are interpolated ONLY from the whitelisted data wrapper `TABLE_SPECS`.
    """
    spec = TABLE_SPECS[table_name]      # all table specs to be fetched from this wrap only
    cols = ("run_id",) + spec.columns   # all cols only acceptable if derived from spec
    
    # interpolating table and fields in now only after derivation
    query = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({vals})").format(
        tbl=sql.Identifier(spec.table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )

    # params:
    params: list[tuple[Any, ...]] = []
    for r in rows:
        tup: list[Any] = [run_id]
        for c in spec.columns:
            tup.append(adapt_staging_value(c, r.get(c)))
        params.append(tuple(tup))
    
    """
    print("Query:", query, "\n")        # shows composed SQL
    if params: print("First row to be inserted:", params[0])    # shows the first row being inserted
    """
     

    if params:
        with conn.cursor() as cur:
            cur.executemany(query, params)  # sequential batch processing