from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from warehouse_pipeline.stage import MappedSquareOrders, StageReject, StageRow
from warehouse_pipeline.stage.derive_fields import normalize_text, quantize_money, to_decimal


def _as_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _money_amount(value: Any) -> Decimal | None:
    if not isinstance(value, dict):
        return None
    amount = value.get("amount")
    dec = to_decimal(amount)
    if dec is None:
        return None
    return quantize_money(dec / Decimal("100"))


def _money_currency(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    currency = value.get("currency")
    if currency is None:
        return None
    return normalize_text(str(currency))


def _decimal_or_none(value: Any) -> Decimal | None:
    dec = to_decimal(value)
    return dec if dec is not None else None


def map_square_orders(orders: Iterable[dict[str, Any]]) -> MappedSquareOrders:
    order_rows: list[StageRow] = []
    order_line_rows: list[StageRow] = []
    tender_rows: list[StageRow] = []
    rejects: list[StageReject] = []

    line_source_ref = 0
    tender_source_ref = 0

    for order_source_ref, order in enumerate(orders, start=1):
        order_id_raw = order.get("id")
        order_id = normalize_text(str(order_id_raw)) if order_id_raw is not None else None

        if order_id is None:
            rejects.append(
                StageReject(
                    table_name="stg_square_orders",
                    source_ref=order_source_ref,
                    raw_payload=order,
                    reason_code="missing_order_id",
                    reason_detail="Square order payload is missing id",
                )
            )
            continue

        net_amounts = order.get("net_amounts")
        if not isinstance(net_amounts, dict):
            net_amounts = {}

        total_money = order.get("total_money")
        net_total_money = net_amounts.get("total_money")
        total_discount_money = order.get("total_discount_money") or net_amounts.get(
            "discount_money"
        )
        total_tax_money = order.get("total_tax_money") or net_amounts.get("tax_money")
        total_tip_money = order.get("total_tip_money")

        order_rows.append(
            StageRow(
                table_name="stg_square_orders",
                source_ref=order_source_ref,
                raw_payload=order,
                values={
                    "order_id": order_id,
                    "location_id": normalize_text(order.get("location_id")),
                    "customer_id": normalize_text(order.get("customer_id")),
                    "state": normalize_text(order.get("state")),
                    "created_at_source": order.get("created_at"),
                    "updated_at_source": order.get("updated_at"),
                    "closed_at_source": order.get("closed_at"),
                    "currency_code": _money_currency(total_money)
                    or _money_currency(net_total_money),
                    "total_money": _money_amount(total_money),
                    "net_total_money": _money_amount(net_total_money),
                    "total_discount_money": _money_amount(total_discount_money),
                    "total_tax_money": _money_amount(total_tax_money),
                    "total_tip_money": _money_amount(total_tip_money),
                },
            )
        )

        for line_number, line in enumerate(_as_dict_list(order.get("line_items")), start=1):
            line_source_ref += 1
            line_uid = normalize_text(str(line.get("uid") or f"line-{line_number}"))

            quantity = _decimal_or_none(line.get("quantity"))
            if quantity is None or quantity <= 0:
                rejects.append(
                    StageReject(
                        table_name="stg_square_order_lines",
                        source_ref=line_source_ref,
                        raw_payload={"order": order, "line": line},
                        reason_code="invalid_line_quantity",
                        reason_detail=f"order {order_id} line {line_uid} has invalid quantity",
                    )
                )
                continue

            order_line_rows.append(
                StageRow(
                    table_name="stg_square_order_lines",
                    source_ref=line_source_ref,
                    raw_payload={"order": order, "line": line},
                    values={
                        "order_id": order_id,
                        "line_uid": line_uid,
                        "catalog_object_id": normalize_text(line.get("catalog_object_id")),
                        "name": normalize_text(line.get("name")),
                        "variation_name": normalize_text(line.get("variation_name")),
                        "quantity": quantity,
                        "base_price_money": _money_amount(line.get("base_price_money")),
                        "gross_sales_money": _money_amount(line.get("gross_sales_money")),
                        "total_discount_money": _money_amount(line.get("total_discount_money")),
                        "total_tax_money": _money_amount(line.get("total_tax_money")),
                        "net_sales_money": _money_amount(line.get("total_money")),
                        "currency_code": _money_currency(line.get("total_money"))
                        or _money_currency(line.get("base_price_money")),
                    },
                )
            )

        for tender_number, tender in enumerate(_as_dict_list(order.get("tenders")), start=1):
            tender_source_ref += 1
            tender_id = normalize_text(str(tender.get("id") or f"tender-{tender_number}"))

            card_details = tender.get("card_details")
            if not isinstance(card_details, dict):
                card_details = {}

            card_obj = card_details.get("card")
            if not isinstance(card_obj, dict):
                card_obj = {}

            tender_rows.append(
                StageRow(
                    table_name="stg_square_tenders",
                    source_ref=tender_source_ref,
                    raw_payload={"order": order, "tender": tender},
                    values={
                        "order_id": order_id,
                        "tender_id": tender_id,
                        "tender_type": normalize_text(tender.get("type")),
                        "card_brand": normalize_text(
                            card_obj.get("card_brand") or card_details.get("card_brand")
                        ),
                        "amount_money": _money_amount(tender.get("amount_money")),
                        "tip_money": _money_amount(tender.get("tip_money")),
                        "currency_code": _money_currency(tender.get("amount_money")),
                    },
                )
            )

    return MappedSquareOrders(
        order_rows=order_rows,
        order_line_rows=order_line_rows,
        tender_rows=tender_rows,
        rejects=rejects,
    )
