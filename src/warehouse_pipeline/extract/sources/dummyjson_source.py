from __future__ import annotations

from datetime import datetime

from warehouse_pipeline.extract.bundles import fetch_live_bundle
from warehouse_pipeline.extract.filters import filter_bundle_to_window
from warehouse_pipeline.extract.source_contract import PullResult
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow
from warehouse_pipeline.stage.derive_fields import (
    derive_order_ts,
    synthetic_order_ts_window_high,
)


class DummyJsonSource:
    """
    (DummyJson does not expose a real updated_at/create_time cursor for carts.)

    - live mode is a native full pull
    - incremental mode is a full pull with a client-side filter on derived order_ts
    """

    source_system = "dummyjson"

    def validate_watermark_column(self, watermark_column: str) -> None:
        """Synthetic time derivation means `watermark_colunm` can only be `order_ts`."""
        if watermark_column != "order_ts":
            raise ValueError(
                "DummyJson only supports watermark_column='order_ts' "
                "via a deterministic derived timestamp."
            )

    def default_high_watermark(
        self,
        *,
        watermark_column: str,
        run_started_at: datetime,
    ) -> datetime | None:
        """
        Validate the watermark_column is `order_ts`
        and return its synthetic upper bound.
        """
        self.validate_watermark_column(watermark_column)
        return synthetic_order_ts_window_high()

    def pull_full(self, *, page_size: int) -> PullResult:
        """Pull all at once for `DummyJson`."""
        bundle = fetch_live_bundle(page_size=page_size)
        return PullResult(
            bundle=bundle,
            meta={
                "source_system": self.source_system,
                "native_incremental": False,
                "selection_strategy": "full_pull",
            },
        )

    def pull_incremental(
        self,
        *,
        page_size: int,
        window: ExtractionWindow,
    ) -> PullResult:
        """
        Pull incrementally locally for `DummyJson` using derived `order_ts`.
        Return a `PullResult`.
        """
        self.validate_watermark_column(window.watermark_column)

        full_bundle = fetch_live_bundle(page_size=page_size)

        def _cart_ts(cart):
            """Base it off of parsed carts."""
            return derive_order_ts(cart_id=cart.id, user_id=cart.userId)

        filtered_bundle, carts_pre_filter = filter_bundle_to_window(
            full_bundle,
            window=window,
            cart_ts_func=_cart_ts,
        )

        return PullResult(
            bundle=filtered_bundle,
            meta={
                "source_system": self.source_system,
                "native_incremental": False,
                "selection_strategy": "full_pull_plus_client_side_filter",
                "watermark_column": window.watermark_column,
                "low": window.low.isoformat(),
                "high": window.high.isoformat(),
                "prior_watermark": (
                    window.prior_watermark.isoformat()
                    if window.prior_watermark is not None
                    else None
                ),
                "overlap_applied_s": window.overlap.total_seconds(),
                "is_first_run": window.is_first_run,
                "carts_pre_filter": carts_pre_filter,
                "carts_post_filter": len(filtered_bundle.carts),
            },
        )


# this mock only exists for DummyJson.
# remove once removing DummyJson as a source.
