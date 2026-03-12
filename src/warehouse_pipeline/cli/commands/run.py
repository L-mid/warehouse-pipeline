from __future__ import annotations

import argparse
from pathlib import Path

from warehouse_pipeline.orchestration import RunSpec, run_pipeline


def register_run_commands(subparsers: argparse._SubParsersAction) -> None:
    """
    Register main facing run command modes + options,
    `live` mode and `snapshot` mode.
    """
    run = subparsers.add_parser("run", help="Run the end-to-end pipeline.")

    run.add_argument(
        "--mode",
        choices=("snapshot", "live"),
        default="snapshot",
        help="Use pinned snapshot files or live DummyJSON HTTP.",
    )
    run.add_argument(
        "--snapshot",
        dest="snapshot_key",
        default="v1",
        help="Snapshot key under data/snapshots/dummyjson/ (snapshot mode only).",
    )
    run.add_argument(
        "--runs-root",
        default="runs",
        help="Directory for per-run artifacts.",
    )
    run.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="HTTP page size for live mode.",
    )
    run.set_defaults(handler=handle_run)


def handle_run(args: argparse.Namespace) -> int:
    """Wrapper to handle runs called from CLI."""

    ## -- init `RunSpec`` to hand to pipeline
    spec = RunSpec(
        mode=args.mode,
        snapshot_key=args.snapshot_key if args.mode == "snapshot" else None,
        runs_root=Path(args.runs_root),
        page_size=args.page_size,
    )
    # pipeline.
    manifest = run_pipeline(spec)

    # default stdout for successful CLI upon usage of this function.
    # not optional.
    print(
        f"run_id={manifest.run_id} "
        f"status={manifest.status} "
        f"mode={manifest.mode} "
        f"manifest={manifest.artifacts['manifest']}"
    )
    # (logger may also print event info to stdout, optional)

    # manifest's `"succeeded"` field chosen to determine success for CLI.
    return 0 if manifest.status == "succeeded" else 1
