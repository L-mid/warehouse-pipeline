# Run contract

A run is considered one end-to-end pipeline execution.

- `run_id`: UUIDv4 string (`"6d0c1a21-9d2b-4e85-9c9b-b54a8b2bd8e1"`).
- `run_mode`: `"snapshot"` or `"live"`.


## Modes

### default snapshot mode
Reads from the repo-pinned files:
- `data/snapshots/dummyjson/<snapshot_id>/users.json`
- `data/snapshots/dummyjson/<snapshot_id>/products.json`
- `data/snapshots/dummyjson/<snapshot_id>/carts.json`

`snapshot_id` examples:
- `v1`      (demo)
- `smoke`   (tiny + fast CI)

### live mode (optional)
Reads from DummyJSON HTTP endpoints:
- `/users`, `/products`, `/carts`

Live mode not required for against CI.


## Runtime artifacts

All per-run artifacts go under:

- `runs/<run_id>/manifest.json`  (required)
- `runs/<run_id>/logs.txt`       (optional)
- `runs/<run_id>/timings.json`   (optional. it
s ok to embed in manifest instead)

