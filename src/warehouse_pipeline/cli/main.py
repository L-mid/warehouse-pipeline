from __future__ import annotations

import argparse
from pathlib import Path
from uuid import UUID
        

from warehouse_pipeline.db.initialize import db_init
from warehouse_pipeline.warehouse.build import build_warehouse
from warehouse_pipeline.db.connect import connect
from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.dq.runner import run_dq

 
def main(argv: list[str] | None = None) -> int:
    """
    A CLI for loading and staging data into a Postgres database.

    The `cmd` options are:
    ## load:   
    Will load and stage the provided sample data into Postgres.
    - `--input` as the path to the data, 
    - `--table` as its respective staging table

    A results summary will print in the terminal upon completion of a load.

    
    ### Example load usage:
    #### For customer sample data:
    - `pipeline load --input data/sample/customers-1000.csv --table stg_customers`

    #### For retail transactions sample data:
    - `pipeline load --input data/sample/retail_transactions.csv --table stg_retail_transactions`

    #### For orders sample data:
    - `pipeline load --input data/sample/orders.csv --table stg_orders`

    #### For order items sample data:
    - `pipeline load --input data/sample/order_items.csv --table stg_order_items`
    

    ## db:     
    Database controlling commands, includes DB initalization functionality. 
    - `init` is the command to reinitalize the DB
    - `--sql` is an optional pointer to which dir contains the SQL file(s) you want to use to reinitalize.

    ## warehouse
    Build transformations using staged tables.
    """
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    # load cmd
    load = sub.add_parser("load", help="Load a file into a staging table (with rejects).")
    load.add_argument("--input", required=True, help="Path to input file (CSV or JSONL).")
    load.add_argument("--table", required=True, choices=["stg_customers", "stg_retail_transactions", "stg_orders", "stg_order_items"]),

    # db cmd
    db = sub.add_parser("db", help="Database utilities.")
    db_sub = db.add_subparsers(dest="db_cmd", required=True)

    db_init_p = db_sub.add_parser("init", help="Initialize DB schema from SQL file(s).")
    db_init_p.add_argument("--sql", default="sql", help="Path to schema SQL file OR a directory of `.sql` files.")

    # warehouse cmd
    warehouse = sub.add_parser("warehouse", help="Warehouse utilities.")
    wh_sub = warehouse.add_subparsers(dest="wh_cmd", required=True)

    wh_build = wh_sub.add_parser("build", help="Build dims/facts from staging runs.")
    wh_build.add_argument("--customers-run-id", default=None)
    wh_build.add_argument("--orders-run-id", default=None)
    wh_build.add_argument("--order-items-run-id", default=None)

    args = p.parse_args(argv)
 

    if args.cmd == "load":
        input_path = Path(args.input)
        with connect() as conn:
            # ingest data
            summary = load_file(conn, input_path=input_path, table_name=args.table)
            # checks
            run_dq(conn, run_id=summary.run_id, table_name=summary.table_name)

        print(summary.render_one_line())
        return 0

    if args.cmd == "db" and args.db_cmd == "init":
        db_init(sql_path=Path(args.sql))
        print(f"Initialized schema from {args.sql}")
        return 0
    
    if args.cmd == "warehouse" and args.wh_cmd == "build":

        def _maybe_uuid(x: str | None) -> UUID | None:
            """Returns `UUID` `run_id` of a staged run if not `None`."""
            return None if not x else UUID(x)

        with connect() as conn:
            used = build_warehouse(
                conn,
                customers_run_id=_maybe_uuid(args.customers_run_id),
                orders_run_id=_maybe_uuid(args.orders_run_id),
                order_items_run_id=_maybe_uuid(args.order_items_run_id),
            )
        
        print(
            "Built warehouse with "
            f"customers_run_id={used['customers_run_id']} "
            f"orders_run_id={used['orders_run_id']} "
            f"order_items_run_id={used['order_items_run_id']}"
        )
        return 0        


    return 2