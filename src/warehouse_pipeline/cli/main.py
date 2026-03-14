from __future__ import annotations

import argparse

from warehouse_pipeline.cli.commands.db import register_db_commands
from warehouse_pipeline.cli.commands.run import register_run_commands


def build_parser() -> argparse.ArgumentParser:
    """Builds all CLI parsers, outside of `main()`."""
    parser = argparse.ArgumentParser(prog="pipeline")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    register_db_commands(subparsers)
    register_run_commands(subparsers)

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    A CLI for loading and staging data into a Postgres database.

    Supported commands are:
    - `run`,    exercises the pipeline.
    - `db`,     database interactions.

    A results summary will print in the terminal upon completion of a command.



    ### Example usage:
    #### initalize the database to yourself locally.
    `pipeline db init`

    #### run the pipeline on a saved snapshot of pre-extracted DummyJson.
    `pipeline run --mode snapshot --snapshot v1`

    #### smoke the pipeline fast on a stable mock extraction.
    `pipeline run --mode snapshot --snapshot smoke`

    #### extract from `DummyJson` live and run the pipeline on that (requires internet).
    `pipeline run --mode live`

    #### run an incremental run on live data
    `pipeline run --mode incremental --since 2024-01-01T00:00:00+00:00 \
    --until 2025-01-01T00:00:00+00:00 --page-size 100`
    """

    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2

    return int(handler(args))  # handlers are responsible for returning their `int` ONLY


if __name__ == "__main__":
    raise SystemExit(main())
