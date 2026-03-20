from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_BASE = ROOT / "data" / "snapshots" / "square_orders"


def money(amount_cents: int, currency: str = "USD") -> dict[str, Any]:
    return {"amount": amount_cents, "currency": currency}


def square_line(
    *,
    uid: str,
    name: str,
    quantity: str,
    catalog_object_id: str,
    base_price_cents: int,
    gross_sales_cents: int,
    net_sales_cents: int,
    total_discount_cents: int = 0,
    total_tax_cents: int = 0,
) -> dict[str, Any]:
    return {
        "uid": uid,
        "catalog_object_id": catalog_object_id,
        "name": name,
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
    out: dict[str, Any] = {
        "id": tender_id,
        "type": tender_type,
        "amount_money": money(amount_cents),
        "tip_money": money(tip_cents),
    }
    if card_brand is not None:
        out["card_details"] = {"card": {"card_brand": card_brand}}
    return out


def square_order(
    *,
    order_id: str,
    state: str,
    created_at: str,
    updated_at: str,
    closed_at: str | None,
    total_money_cents: int,
    net_total_money_cents: int,
    line_items: list[dict[str, Any]],
    tenders: list[dict[str, Any]],
    total_discount_cents: int = 0,
    total_tax_cents: int = 0,
    total_tip_cents: int = 0,
    location_id: str = "LOC-1",
    customer_id: str | None = "CUST-1",
    currency: str = "USD",
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
        "line_items": line_items,
        "tenders": tenders,
    }


def write_snapshot(snapshot_key: str, orders: list[dict[str, Any]]) -> None:
    root = SNAPSHOT_BASE / snapshot_key
    root.mkdir(parents=True, exist_ok=True)
    path = root / "orders.json"
    path.write_text(
        json.dumps({"orders": orders, "total": len(orders)}, indent=2),
        encoding="utf-8",
    )
    print(f"wrote {path}")


def build_smoke_orders() -> list[dict[str, Any]]:
    return [
        square_order(
            order_id="ord-100",
            state="COMPLETED",
            created_at="2026-03-10T10:00:00Z",
            updated_at="2026-03-10T10:05:00Z",
            closed_at="2026-03-10T10:05:00Z",
            total_money_cents=2300,
            net_total_money_cents=2000,
            total_discount_cents=200,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-espresso",
                    catalog_object_id="item-espresso",
                    name="Espresso",
                    quantity="1",
                    base_price_cents=1200,
                    gross_sales_cents=1200,
                    net_sales_cents=1200,
                ),
                square_line(
                    uid="line-cookie",
                    catalog_object_id="item-cookie",
                    name="Cookie",
                    quantity="2",
                    base_price_cents=400,
                    gross_sales_cents=800,
                    net_sales_cents=800,
                ),
            ],
            tenders=[
                square_tender(
                    tender_id="tender-100",
                    tender_type="CARD",
                    amount_cents=2300,
                    card_brand="VISA",
                )
            ],
        ),
        square_order(
            order_id="ord-101",
            state="COMPLETED",
            created_at="2026-03-10T11:00:00Z",
            updated_at="2026-03-10T11:05:00Z",
            closed_at="2026-03-10T11:05:00Z",
            total_money_cents=1100,
            net_total_money_cents=1000,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-bagel",
                    catalog_object_id="item-bagel",
                    name="Bagel",
                    quantity="1",
                    base_price_cents=1000,
                    gross_sales_cents=1000,
                    net_sales_cents=1000,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-101",
                    tender_type="CASH",
                    amount_cents=1100,
                )
            ],
        ),
    ]


def build_sandbox_v1_orders() -> list[dict[str, Any]]:
    # For now using a slightly larger pinned set.
    # Replace later with a real pulled Square sandbox snapshots written to disk?
    # would work but would need to commit it to repo
    return build_smoke_orders() + [
        square_order(
            order_id="ord-102",
            state="COMPLETED",
            created_at="2026-03-11T09:00:00Z",
            updated_at="2026-03-11T09:02:00Z",
            closed_at="2026-03-11T09:02:00Z",
            total_money_cents=1500,
            net_total_money_cents=1500,
            line_items=[
                square_line(
                    uid="line-muffin",
                    catalog_object_id="item-muffin",
                    name="Muffin",
                    quantity="1",
                    base_price_cents=1500,
                    gross_sales_cents=1500,
                    net_sales_cents=1500,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-102",
                    tender_type="CARD",
                    amount_cents=1500,
                    card_brand="MASTERCARD",
                )
            ],
        )
    ]


def main() -> None:
    write_snapshot("smoke", build_smoke_orders())
    write_snapshot("sandbox_v1", build_sandbox_v1_orders())


if __name__ == "__main__":
    main()
