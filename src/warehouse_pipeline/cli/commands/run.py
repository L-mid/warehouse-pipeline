from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path

from warehouse_pipeline.orchestration import RunSpec, run_pipeline
from warehouse_pipeline.orchestration.contract import DEFAULT_INCREMENTAL_OVERLAP_WINDOW


def register_run_commands(subparsers: argparse._SubParsersAction) -> None:
    """
    Register main facing run command modes + options,
    `live` mode, `snapshot` mode, and `incremental` mode.
    """
    run = subparsers.add_parser("run", help="Run the end-to-end pipeline.")

    run.add_argument(
        "--mode",
        choices=("snapshot", "live", "incremental"),
        default="snapshot",
        help="snapshot=pinned files, live=full HTTP pull, incremental=watermark-based.",
    )
    run.add_argument(
        "--snapshot",
        dest="snapshot_key",
        default="sandbox_v1",
        help="Snapshot key under data/snapshots/square_orders/ (snapshot mode only).",
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
        help="HTTP page size for Square SearchOrders.",
    )

    ## -- incremental options only
    run.add_argument(
        "--watermark-column",
        default="updated_at",
        help="Incremental cursor column. Currently uses updated_at.",
    )
    run.add_argument(
        "--since",
        type=datetime.fromisoformat,
        default=None,
        help="Explicit low watermark (ISO timestamp).",
    )
    run.add_argument(
        "--until",
        type=datetime.fromisoformat,
        default=None,
        help="Explicit high watermark (ISO timestamp).",
    )
    run.add_argument(
        "--overlap",
        type=_parse_overlap,
        default=DEFAULT_INCREMENTAL_OVERLAP_WINDOW,
        help="Examples: '7d', '1h', '30m', '2d6h30m'. Default: 7d.",
    )

    # temporarliy skip
    run.add_argument(
        "--with-dq",
        action="store_true",
        help="Run DQ + gate checks. Left off for refactor",
    )
    run.add_argument(
        "--with-warehouse",
        action="store_true",
        help="Run warehouse build + publish. Left off for refactor.",
    )

    run.set_defaults(handler=handle_run)


def _parse_overlap(value: str) -> timedelta:
    """Parse shorthand like '1h', '30m', '2h30m' into timedelta."""
    # re for shorthand
    value = value.strip().lower()
    parts = re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?", value)

    if not parts or not any(group is not None for group in parts.groups()):
        raise argparse.ArgumentTypeError(
            f"Cannot parse overlap '{value}'. Use e.g. '7d', '30m', '2d6h30m'."
        )
    days = int(parts.group(1) or 0)
    hours = int(parts.group(2) or 0)
    minutes = int(parts.group(3) or 0)
    return timedelta(days=days, hours=hours, minutes=minutes)


def handle_run(args: argparse.Namespace) -> int:
    """Wrapper to handle runs called from CLI."""

    ## -- init `RunSpec`` to hand to pipeline
    spec = RunSpec(
        mode=args.mode,
        source_system="square_orders",
        snapshot_key=args.snapshot_key if args.mode == "snapshot" else None,
        runs_root=Path(args.runs_root),
        page_size=args.page_size,
        watermark_column=args.watermark_column,
        since=args.since,
        until=args.until,
        overlap_window=args.overlap,
        run_dq=args.with_dq,
        run_transforms=args.with_warehouse,
        publish_views=args.with_warehouse,
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

    # special prints for extraction window
    if manifest.extraction_window:
        ew = manifest.extraction_window
        print(f"  window: {ew.get('low', 'n/a')} to {ew.get('high', 'n/a')}")

    # manifest's `"succeeded"` field chosen to determine success for CLI.
    return 0 if manifest.status == "succeeded" else 1
