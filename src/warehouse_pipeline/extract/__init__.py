from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.extract.bundles import (
    ExtractBundle,
    fetch_live_bundle,
    read_snapshot_bundle,
    snapshot_root_for_key,
    write_snapshot_bundle,
)
from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient



def extract_dummyjson_snapshots(
    *,
    snapshot_root: Path,
    page_size: int = 100,
    client: DummyJsonClient | None = None,
) -> dict[str, Path]:
    """
    Fetch all `DummyJSON` resources in live mode, and write pinned snapshots.

    Returns a dict like:
        `{
            "users": Path(.../users.json),
            "products": Path(.../products.json),
            "carts": Path(.../carts.json),
        }`
    For use as snapshots further down the line.
    """
    bundle = fetch_live_bundle(page_size=page_size, client=client)
    return write_snapshot_bundle(bundle, snapshot_root=snapshot_root)


__all__ = [
    "DummyJsonClient",
    "ExtractBundle",
    "extract_dummyjson_snapshots",
    "fetch_live_bundle",
    "read_snapshot_bundle",
    "snapshot_root_for_key",
    "write_snapshot_bundle",
]



