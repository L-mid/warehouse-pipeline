from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class StagingTableSpec:
    table_name: str
    columns: tuple[str, ...]
    key_cols: tuple[str, ...]
    json_cols: frozenset[str] = field(default_factory=frozenset)

    @property
    def work_table_name(self) -> str:
        return f"_work_{self.table_name}"


# wrap all staging specs for data together.
TABLE_SPECS: dict[str, StagingTableSpec] = {
    "stg_square_orders": StagingTableSpec(
        table_name="stg_square_orders",
        columns=(
            "order_id",
            "location_id",
            "customer_id",
            "state",
            "created_at_source",
            "updated_at_source",
            "closed_at_source",
            "currency_code",
            "total_money",
            "net_total_money",
            "total_discount_money",
            "total_tax_money",
            "total_tip_money",
        ),
        key_cols=("order_id",),
    ),
    "stg_square_order_lines": StagingTableSpec(
        table_name="stg_square_order_lines",
        columns=(
            "order_id",
            "line_uid",
            "catalog_object_id",
            "name",
            "variation_name",
            "quantity",
            "base_price_money",
            "gross_sales_money",
            "total_discount_money",
            "total_tax_money",
            "net_sales_money",
            "currency_code",
        ),
        key_cols=("order_id", "line_uid"),
    ),
    "stg_square_tenders": StagingTableSpec(
        table_name="stg_square_tenders",
        columns=(
            "order_id",
            "tender_id",
            "tender_type",
            "card_brand",
            "amount_money",
            "tip_money",
            "currency_code",
        ),
        key_cols=("order_id", "tender_id"),
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
