# Run contract

A run is one full end-to-end pipeline execution.

Each run has:
- a `run_id` (UUID)
- a row in `run_ledger`
- staging rows tagged with that `run_id`
- DQ metric rows tagged with that `run_id`
- a per-run artifact directory under `runs/<run_id>/`


## Run modes

### Snapshot mode
Reads pinned files from the repo:

- `data/snapshots/dummyjson/<snapshot_key>/users.json`
- `data/snapshots/dummyjson/<snapshot_key>/products.json`
- `data/snapshots/dummyjson/<snapshot_key>/carts.json`

Current snapshot keys:
- `smoke`   — tiny, fast, stable, used by `make demo`
- `v1`      — fuller manual example of a snapshot


### Live mode
Reads from `DummyJSON` HTTP endpoints:

- `/users`
- `/products`
- `/carts`

Live mode exercises the external extract path, but it is not the default demo path.


## Runtime artifacts

Each run writes a small artifact bundle:

- `runs/<run_id>/manifest.json`     — required summary artifact
- `runs/<run_id>/logs.jsonl`        — structured per-phase/event log

The manifest is the human-readable summary for a run.
The log file is the detailed event stream, it's used for debugging.
