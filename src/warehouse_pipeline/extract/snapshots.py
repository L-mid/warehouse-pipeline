from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.extract.snapshot_store import SnapshotStore

DEFAULT_SNAPSHOT_BASE_DIR = (
    Path(__file__).resolve().parents[3] / "data" / "snapshots" / "square_orders"
)


def snapshot_root_for_key(snapshot_key: str, *, base_dir: Path | None = None) -> Path:
    """Snapshot root fetching."""
    root = (base_dir or DEFAULT_SNAPSHOT_BASE_DIR).resolve()
    return root / snapshot_key


def read_snapshot_extract(
    *,
    snapshot_root: Path,
    snapshot_key: str | None = None,
) -> RawExtract:
    """
    Read pinned Square raw orders from:
      data/snapshots/square_orders/<snapshot_key>/orders.json
    """
    root = snapshot_root.resolve()
    store = SnapshotStore(root)

    payload = store.read_json("orders")
    orders = payload.get("orders")
    if not isinstance(orders, list):
        raise ValueError(f"{store.path_for('orders')} must contain an 'orders' list")

    typed_orders: list[dict] = []
    for row in orders:
        if not isinstance(row, dict):
            raise ValueError("orders.json contains a non-object order payload")
        typed_orders.append(row)

    return RawExtract(
        source_system="square_orders",
        mode="snapshot",
        snapshot_key=snapshot_key,
        entities={"orders": tuple(typed_orders)},
        source_paths={"orders": str(store.path_for("orders"))},
        totals={"orders": len(typed_orders)},
        pages_fetched={"orders": 1},
        page_size=None,
    )


def write_snapshot_extract(extract: RawExtract, *, snapshot_root: Path) -> dict[str, Path]:
    """
    Persist raw Square orders as extracted in dict.
    """
    store = SnapshotStore(snapshot_root.resolve())
    orders = list(extract.entities.get("orders", ()))

    out: dict[str, Path] = {}
    out["orders"] = store.write_json(
        "orders",
        {
            "orders": orders,
            "total": len(orders),
        },
    )
    return out
