from __future__ import annotations

import argparse
from pathlib import Path

from warehouse_pipeline.db.initialize import initialize_database


def register_db_commands(subparsers: argparse._SubParsersAction) -> None:
    """Initalize argparse parsers for CLI `db` commands."""
    db = subparsers.add_parser("db", help="Database utilities.")
    db_sub = db.add_subparsers(dest="db_cmd", required=True)

    init_p = db_sub.add_parser("init", help="Initialize DB schema from SQL file(s).")
    init_p.add_argument(
        "--sql",
        default="sql/schema",
        help="Path to schema SQL file or directory.",
    )
    init_p.set_defaults(handler=handle_db_init)


def handle_db_init(args: argparse.Namespace) -> int:
    """Handler for re-initalizing the `db` from CLI."""
    initialize_database(sql_path=Path(args.sql))
    print(f"Initialized schema from {args.sql}")
    return 0
