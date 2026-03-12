from __future__ import annotations

from collections.abc import Iterable

from warehouse_pipeline.extract.models import DummyProduct
from warehouse_pipeline.stage import MappedProducts, ProductLookupItem, StageReject, StageRow
from warehouse_pipeline.stage.derive_fields import (
    derive_product_discount_fraction,
    derive_sku,
    normalize_text,
    quantize_money,
    to_decimal,
)


def map_products(products: Iterable[DummyProduct]) -> MappedProducts:
    """
    Map validated `DummyJSON` products into `stg_products` rows and a lookup.
    """
    rows: list[StageRow] = []
    rejects: list[StageReject] = []
    product_lookup: dict[int, ProductLookupItem] = {}

    for source_ref, product in enumerate(products, start=1):
        raw_payload = product.model_dump(mode="python")

        title = normalize_text(product.title)
        category = normalize_text(product.category)
        if title is None or category is None:
            rejects.append(
                StageReject(
                    table_name="stg_products",
                    source_ref=source_ref,
                    raw_payload=raw_payload,
                    reason_code="missing_product_fields",
                    reason_detail="product could not be mapped because title or category is blank",
                )
            )
            continue

        sku = derive_sku(product_id=product.id, category=category, title=title)
        price_usd = quantize_money(product.price)
        discount_pct = derive_product_discount_fraction(product.discountedTotal)
        brand = normalize_text(product.brand)
        rating = to_decimal(product.rating) if product.rating is not None else None

        rows.append(
            StageRow(
                table_name="stg_products",
                source_ref=source_ref,
                raw_payload=raw_payload,
                values={
                    "product_id": product.id,
                    "sku": sku,
                    "title": title,
                    "brand": brand,
                    "category": category,
                    "price_usd": price_usd,
                    "discount_pct": discount_pct,
                    "rating": rating,
                    "stock": product.stock,
                },
            )
        )

        # Keep the first seen value stored so lookup resolution matches the
        # work table duplicate winner first seen rule.
        product_lookup.setdefault(
            product.id,
            ProductLookupItem(
                product_id=product.id,
                sku=sku,
                title=title,
                category=category,
                unit_price_usd=price_usd,
                discount_pct=discount_pct,
            ),
        )

    return MappedProducts(rows=rows, rejects=rejects, product_lookup=product_lookup)
