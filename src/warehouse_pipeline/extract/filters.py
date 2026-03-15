from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

from warehouse_pipeline.extract.bundles import ExtractBundle
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow

T = TypeVar("T")


def _filter_items(
    items: tuple[T, ...],
    *,
    get_ts: Callable[[T], datetime | None],
    low: datetime,
    high: datetime,
) -> tuple[T, ...]:
    """Keep items where `low <= ts < high`. Items with `ts=None` are dropped."""
    return tuple(item for item in items if (ts := get_ts(item)) is not None and low <= ts < high)


def filter_bundle_to_window(
    bundle: ExtractBundle,
    *,
    window: ExtractionWindow,
    cart_ts_func: Callable | None = None,
) -> tuple[ExtractBundle, int]:
    """
    Return a new bundle containing only `orders/order_items`
    inside the extraction window.

    Returns `(filtered_bundle, total_source_rows)`.
    """

    if cart_ts_func is None:
        raise ValueError("`cart_ts_func` is required to derive timestamps.")

    total_source_carts = len(bundle.carts)

    filtered_carts = _filter_items(
        bundle.carts,
        get_ts=cart_ts_func,
        low=window.low,
        high=window.high,
    )

    return (
        ExtractBundle(
            mode=bundle.mode,
            users=bundle.users,
            products=bundle.products,
            carts=filtered_carts,
            snapshot_key=bundle.snapshot_key,
            source_paths=bundle.source_paths,
            totals={
                **bundle.totals,
                "carts_pre_filter": total_source_carts,
                "carts": len(filtered_carts),
            },
            pages_fetched=bundle.pages_fetched,
            page_size=bundle.page_size,
        ),
        total_source_carts,
    )
