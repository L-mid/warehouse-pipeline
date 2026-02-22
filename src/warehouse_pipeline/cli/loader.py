from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.ingest_runs import insert_ingest_run, update_ingest_run_status
from warehouse_pipeline.db.reject_writers import RejectInsert, insert_reject_rows
from warehouse_pipeline.db.staging_writers import insert_staging_rows, TABLE_SPECS
from warehouse_pipeline.ingest.readers import stream_csv_dict_rows, stream_jsonl_dict_rows
from warehouse_pipeline.ingest.summary import LoadSummary
from warehouse_pipeline.parsing.registry import RejectRowProto, RowParserProto, get_table_spec, TableSpec


BATCH_SIZE = 500        # config: increase or decrease.


def _reason_code_to_text(x: Any) -> str:
    """
    Converts enum-like (value `__attr__`) or plain strings of a given value to `str`. 
    """
    # supports enum-like (value attr) or plain strings
    if hasattr(x, "value"):
        return str(getattr(x, "value"))
    return str(x)


def load_file(conn: Connection, *, input_path: Path, table_name: str) -> LoadSummary:
    """
    End-to-end file loading orchestrator:
      - ingest a run (running), 
      - stream rows, 
      - parse each row, 
      - insert staging and rejects appropriately,
      - and update run `status` appropriately.

    Raises on infra related exceptions (DB issues/bad connection, runtime errors, etc.).
    Will not raise on invalid data (will instead inject a `rejected_row` to table 'rejected_rows').
    """


    spec: TableSpec = None
    # collect appropriate table information.
    spec = get_table_spec(table_name)

    
    if spec.input_format == "csv":
        row_iter = stream_csv_dict_rows(input_path)
    else:
        row_iter = stream_jsonl_dict_rows(input_path) 

    ## -- create run ledger, committed immediately
    run_id: UUID = insert_ingest_run(conn, input_path=input_path, table_name=table_name)
    conn.commit()

    total = loaded = rejected = 0

    staged_batch: list[Mapping[str, Any]] = []
    reject_batch: list[RejectInsert] = []       

    ## -- Begin transformations
    try:
        for source_row, raw in row_iter:
            total += 1

            ## -- Parse a row. Any exception here is a bug and should fail the run.
            res = spec.parser.parse(raw, source_row=source_row)

            ## -- Route each row to staging vs rejection.
            if hasattr(res, "to_mapping"):
                loaded += 1
                # stage valid
                m = dict(cast(Any, res).to_mapping())
                # inject the lineage metadata required by a staging table if it needs it
                if "source_row" in TABLE_SPECS[table_name].columns:
                    m["source_row"] = source_row
                staged_batch.append(m)

            else:
                rejected += 1
                # reject invalid
                r = cast(RejectRowProto, res)
                reject_batch.append(
                    RejectInsert(
                        table_name=table_name,
                        source_row=int(r.source_row),
                        raw_payload=dict(r.raw_payload),
                        reason_code=_reason_code_to_text(r.reason_code),
                        reason_detail=str(r.reason_detail),
                    )
                )
 
            ## -- flush batches to disk.
            if len(staged_batch) >= BATCH_SIZE:
                insert_staging_rows(conn, table_name=table_name, run_id=run_id, rows=staged_batch)
                staged_batch.clear()

            if len(reject_batch) >= BATCH_SIZE:
                insert_reject_rows(conn, run_id=run_id, rejects=reject_batch)
                reject_batch.clear()

        ## -- flush any remainder left from last epoch
        insert_staging_rows(conn, table_name=table_name, run_id=run_id, rows=staged_batch)
        insert_reject_rows(conn, run_id=run_id, rejects=reject_batch)

        ## -- run success!
        update_ingest_run_status(conn, run_id=run_id, status="succeeded")
        conn.commit()

        # return for printing in cli terminal later
        return LoadSummary(     
            run_id=run_id,
            table_name=table_name,
            input_path=str(input_path),
            total=total,
            loaded=loaded,
            rejected=rejected,
        )  

    except Exception:
        # revert all changes (excluding run ledger)
        conn.rollback()
        ## -- Update that the run failed (and separate txn)
        update_ingest_run_status(conn, run_id=run_id, status="failed")
        conn.commit()
        raise

