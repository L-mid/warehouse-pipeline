from __future__ import annotations

import argparse
from pathlib import Path

from warehouse_pipeline.db.initialize import db_init
from warehouse_pipeline.db.connect import connect
from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.dq.runner import run_dq

 
def main(argv: list[str] | None = None) -> int:
    """
    Note: add usage exs and notes about the cli later.
    """
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    load = sub.add_parser("load", help="Load a file into a staging table (with rejects).")
    load.add_argument("--input", required=True, help="Path to input file (CSV or JSONL).")
    load.add_argument("--table", required=True, choices=["stg_customers", "stg_retail_transactions"])

    dq = sub.add_parser("dq", help="Run DQ checks for an existing run_id + table.")
    dq.add_argument("--run-id", required=True, help="Run UUID (from `ingest_runs.run_id`).")
    dq.add_argument("--table", required=True, choices=["stg_customers", "stg_retail_transactions"])

    db = sub.add_parser("db", help="Database utilities.")
    db_sub = db.add_subparsers(dest="db_cmd", required=True)

    db_init_p = db_sub.add_parser("init", help="Initialize DB schema from an SQL file.")
    db_init_p.add_argument("--sql", default="sql", help="Path to schema SQL file OR a directory of `.sql` files.")

    args = p.parse_args(argv)


    if args.cmd == "load":
        input_path = Path(args.input)
        with connect() as conn:
            summary = load_file(conn, input_path=input_path, table_name=args.table)

            run_dq(conn, run_id=summary.run_id, table_name=summary.table_name)

        print(summary.render_one_line())
        return 0

    if args.cmd == "db" and args.db_cmd == "init":
        db_init(sql_path=Path(args.sql))
        print(f"Initialized schema from {args.sql}")
        return 0


    return 2