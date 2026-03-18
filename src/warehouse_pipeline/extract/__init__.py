from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.extract.snapshots import (
    read_snapshot_extract,
    snapshot_root_for_key,
    write_snapshot_extract,
)
from warehouse_pipeline.extract.sources.square_orders_source import SquareOrdersSource


def extract_square_snapshots(
    *,
    snapshot_root: Path,
    page_size: int = 100,
    adapter: SquareOrdersSource | None = None,
) -> dict[str, Path]:
    """
    Fetch Square orders live, and persist pinned raw snapshots.
    """
    src = adapter or SquareOrdersSource.from_env()
    result = src.pull_full(page_size=page_size)
    return write_snapshot_extract(result.extract, snapshot_root=snapshot_root)


__all__ = [
    "RawExtract",
    "SquareOrdersSource",
    "extract_square_snapshots",
    "read_snapshot_extract",
    "snapshot_root_for_key",
    "write_snapshot_extract",
]
