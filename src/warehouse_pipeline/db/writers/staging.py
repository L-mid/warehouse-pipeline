from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class StagingTableSpec:
    """
    Whitelisted staging table SQL values.

    - `columns` excludes `run_id` and `created_at`.
    - `key_cols` are the per-run key columns (excluding `run_id`). 
    They match this staging PK shape:
    `PRIMARY KEY (run_id, *key_cols)`.
    """

    table_name: str
    columns: tuple[str, ...]
    key_cols: tuple[str, ...]
    json_cols: frozenset[str] = field(default_factory=frozenset)

    @property
    def work_table_name(self) -> str:
        # here, temp/work table name is derived from the whitelisted staging `table_name` only.
        return f"_work_{self.table_name}"


# wrap all staging specs for data together.
TABLE_SPECS: dict[str, StagingTableSpec] = {
    "stg_customers": StagingTableSpec(
        table_name="stg_customers",
        columns=(
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "phone",
            "city",
            "country",
            "company",
        ),
        key_cols=("customer_id",),
    ),
    "stg_products": StagingTableSpec(
        table_name="stg_products",
        columns=(
            "product_id",
            "sku",
            "title",
            "brand",
            "category",
            "price_usd",
            "discount_pct",
            "rating",
            "stock",
        ),
        key_cols=("product_id",),
    ),
    "stg_orders": StagingTableSpec(
        table_name="stg_orders",
        columns=(
            "order_id",
            "customer_id",
            "order_ts",
            "country",
            "status",
            "total_usd",
            "total_products",
            "total_quantity",
        ),
        key_cols=("order_id",),
    ),
    "stg_order_items": StagingTableSpec(
        table_name="stg_order_items",
        columns=(
            "order_id",
            "line_id",
            "product_id",
            "sku",
            "qty",
            "unit_price_usd",
            "discount_pct",
            "gross_usd",
            "net_usd",
        ),
        key_cols=("order_id", "line_id"),
    ),
}


def get_staging_spec(table_name: str) -> StagingTableSpec:
    """Fetch table from allowlisted `TABLESPECS,` or raise if not found."""
    try:
        return TABLE_SPECS[table_name]
    except KeyError as exc:
        allowed = ", ".join(sorted(TABLE_SPECS))
        raise KeyError(f"Unknown staging table {table_name!r}. Allowed: {allowed}") from exc


def adapt_staging_value(spec: StagingTableSpec, col: str, value: Any) -> Any:  # any value in or out
    """Adapt python values to DB types (e.g., `jsonb`)."""
    if col in spec.json_cols and value is not None:
        return Jsonb(value)
    return value
