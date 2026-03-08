from __future__ import annotations

from typing import Iterable

from warehouse_pipeline.extract.models import DummyCart
from warehouse_pipeline.stage import MappedCarts, ProductLookup, StageReject, StageRow, UserLookup
from warehouse_pipeline.stage.derive_fields import (
    derive_gross_usd,
    derive_line_discount_pct,
    derive_net_usd,
    derive_order_status,
    derive_order_ts,
    quantize_money,
)



def map_carts(
    carts: Iterable[DummyCart],
    *,
    product_lookup: ProductLookup,
    user_lookup: UserLookup | None = None,
) -> MappedCarts:
    """
    Map validated DummyJSON carts into `stg_orders` and `stg_order_items`.

    Note:
    Orders are still staged even when one or more line items are rejected. That
    keeps the pipeline debuggable for now: the order exists, and the missing/bad lines
    are visible in `reject_rows`.
    """
    order_rows: list[StageRow] = []
    order_item_rows: list[StageRow] = []
    rejects: list[StageReject] = []
    line_source_ref = 0    


    for order_source_ref, cart in enumerate(carts, start=1):
        raw_cart = cart.model_dump(mode="python")
        user_info = user_lookup.get(cart.userId) if user_lookup is not None else None

        order_rows.append(
            StageRow(
                table_name="stg_orders",
                source_ref=order_source_ref,
                raw_payload=raw_cart,
                values={
                    "order_id": cart.id,
                    "customer_id": cart.userId,
                    "order_ts": derive_order_ts(cart_id=cart.id, user_id=cart.userId),
                    "country": user_info.country if user_info else None,
                    "status": derive_order_status(
                        cart_id=cart.id,
                        total_products=cart.totalProducts,
                        total_quantity=cart.totalQuantity,
                    ),
                    "total_usd": quantize_money(cart.discountedTotal),
                    "total_products": cart.totalProducts,
                    "total_quantity": cart.totalQuantity,
                },
            )
        )

        for line_id, item in enumerate(cart.products, start=1):
            line_source_ref += 1
            raw_line = {
                "cart": raw_cart,
                "line": item.model_dump(mode="python"),
                "line_id": line_id,
            }

            if item.quantity <= 0:
                rejects.append(
                    StageReject(
                        table_name="stg_order_items",
                        source_ref=line_source_ref,
                        raw_payload=raw_line,
                        reason_code="invalid_quantity",
                        reason_detail=f"cart line {line_id} has non-positive quantity={item.quantity}",
                    )
                )
                continue

            product = product_lookup.get(item.id)
            if product is None:
                rejects.append(
                    StageReject(
                        table_name="stg_order_items",
                        source_ref=line_source_ref,
                        raw_payload=raw_line,
                        reason_code="unknown_product",
                        reason_detail=f"product_id {item.id} was referenced by cart {cart.id} but not found in the product lookup",
                    )
                )
                continue

            discount_pct = derive_line_discount_pct(
                line_total=item.total,
                discounted_line_total=item.discountedPrice,
            )
            gross_usd = derive_gross_usd(quantity=item.quantity, unit_price_usd=item.price)
            net_usd = derive_net_usd(
                gross_usd=gross_usd,
                discount_pct=discount_pct,
                discounted_line_total=item.discountedPrice,
            )

            order_item_rows.append(
                StageRow(
                    table_name="stg_order_items",
                    source_ref=line_source_ref,
                    raw_payload=raw_line,
                    values={
                        "order_id": cart.id,
                        "line_id": line_id,
                        "product_id": item.id,
                        "sku": product.sku,
                        "qty": item.quantity,
                        "unit_price_usd": quantize_money(item.price),
                        "discount_pct": discount_pct,
                        "gross_usd": gross_usd,
                        "net_usd": net_usd,
                    },
                )
            )


    return MappedCarts(
        order_rows=order_rows,
        order_item_rows=order_item_rows,
        rejects=rejects,
    )       


