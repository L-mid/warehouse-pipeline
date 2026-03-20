from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.run_ledger import RunMode, RunStart, create_run
from warehouse_pipeline.publish.views import PublishResult, apply_views
from warehouse_pipeline.stage import MappedSquareOrders, StageTableLoadResult
from warehouse_pipeline.stage.load import load_square_batches
from warehouse_pipeline.stage.map_square_orders import map_square_orders
from warehouse_pipeline.transform.warehouse_build import WarehouseBuildResult, build_warehouse


def money(amount_cents: int, currency: str = "USD") -> dict[str, Any]:
    return {"amount": amount_cents, "currency": currency}


def square_line(
    *,
    uid: str,
    name: str,
    quantity: str = "1",
    catalog_object_id: str | None = None,
    variation_name: str | None = None,
    base_price_cents: int,
    gross_sales_cents: int,
    total_discount_cents: int = 0,
    total_tax_cents: int = 0,
    net_sales_cents: int,
) -> dict[str, Any]:
    return {
        "uid": uid,
        "catalog_object_id": catalog_object_id,
        "name": name,
        "variation_name": variation_name,
        "quantity": quantity,
        "base_price_money": money(base_price_cents),
        "gross_sales_money": money(gross_sales_cents),
        "total_discount_money": money(total_discount_cents),
        "total_tax_money": money(total_tax_cents),
        "total_money": money(net_sales_cents),
    }


def square_tender(
    *,
    tender_id: str,
    tender_type: str,
    amount_cents: int,
    tip_cents: int = 0,
    card_brand: str | None = None,
) -> dict[str, Any]:
    tender: dict[str, Any] = {
        "id": tender_id,
        "type": tender_type,
        "amount_money": money(amount_cents),
        "tip_money": money(tip_cents),
    }
    if card_brand is not None:
        tender["card_details"] = {"card": {"card_brand": card_brand}}
    return tender


def square_order(
    *,
    order_id: str,
    state: str,
    created_at: str,
    updated_at: str,
    closed_at: str | None,
    total_money_cents: int,
    net_total_money_cents: int,
    total_discount_cents: int = 0,
    total_tax_cents: int = 0,
    total_tip_cents: int = 0,
    location_id: str = "LOC-1",
    customer_id: str | None = "CUST-1",
    currency: str = "USD",
    line_items: Sequence[dict[str, Any]] = (),
    tenders: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "id": order_id,
        "location_id": location_id,
        "customer_id": customer_id,
        "state": state,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at,
        "total_money": money(total_money_cents, currency),
        "total_discount_money": money(total_discount_cents, currency),
        "total_tax_money": money(total_tax_cents, currency),
        "total_tip_money": money(total_tip_cents, currency),
        "net_amounts": {
            "total_money": money(net_total_money_cents, currency),
            "discount_money": money(total_discount_cents, currency),
            "tax_money": money(total_tax_cents, currency),
        },
        "line_items": list(line_items),
        "tenders": list(tenders),
    }


def write_square_snapshot(snapshot_root: Path, *, orders: Sequence[dict[str, Any]]) -> Path:
    snapshot_root.mkdir(parents=True, exist_ok=True)
    path = snapshot_root / "orders.json"
    path.write_text(
        json.dumps({"orders": list(orders), "total": len(orders)}, indent=2),
        encoding="utf-8",
    )
    return path


def create_stage_run(
    conn: Connection,
    *,
    orders: Sequence[dict[str, Any]],
    mode: RunMode = "snapshot",
) -> tuple[UUID, MappedSquareOrders, dict[str, StageTableLoadResult]]:
    run_id = create_run(
        conn,
        entry=RunStart(
            mode=mode,
            source_system="square_orders",
            snapshot_key="tests/square_snapshot" if mode == "snapshot" else None,
            git_sha="test-sha",
            args_json={"test": "integration"},
        ),
    )
    mapped = map_square_orders(orders)
    stage_results = load_square_batches(conn, run_id=run_id, square=mapped)
    return run_id, mapped, stage_results


def build_publish_run(
    conn: Connection,
    *,
    orders: Sequence[dict[str, Any]],
    mode: RunMode = "snapshot",
) -> tuple[
    UUID,
    MappedSquareOrders,
    dict[str, StageTableLoadResult],
    WarehouseBuildResult,
    PublishResult,
]:
    run_id, mapped, stage_results = create_stage_run(conn, orders=orders, mode=mode)
    build_result = build_warehouse(conn, run_id=run_id)
    publish_result = apply_views(conn)
    return run_id, mapped, stage_results, build_result, publish_result
