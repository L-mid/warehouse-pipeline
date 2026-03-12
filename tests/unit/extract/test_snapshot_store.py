from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.extract.snapshot_store import SnapshotStore


def test_snapshot_store_roundtrip(tmp_path: Path) -> None:
    """
    `SnapshotsStore`'s read and writes can write a provided `name` and `payload`
    into a valid snapshot, and then read them back properly.
    """
    store = SnapshotStore(tmp_path)

    # usersish field as an example.
    path = store.write_json(
        "users",
        {
            "users": [{"id": 1, "firstName": "Ada", "lastName": "Lovelace"}],
            "total": 1,
            "skip": 0,
            "limit": 100,
        },
    )

    loaded = store.read_json("users")

    assert path.name == "users.json"  # saved where expected.
    assert loaded["total"] == 1
    assert loaded["users"][0]["id"] == 1
