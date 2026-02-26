from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, cast
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.ingest_runs import insert_ingest_run, update_ingest_run_status
from warehouse_pipeline.db.reject_writers import RejectInsert, insert_reject_rows
from warehouse_pipeline.db.staging_writers import TABLE_SPECS
from warehouse_pipeline.db.work_tables import WorkRow, finalize_work_to_staging, insert_work_rows, prepare_work_table
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
      - Create an `ingest_run` row (which is committed immediately),
      - Stream rows from CSV/JSONL,
      - Parse and validate each row,
            - invalid rows -> `reject_rows`,
            - valid rows -> temp/work table (with no uniqueness constraints),
      - Finalized which rows are staged:
            - dedupe from work -> staging 
            - duplicates -> `reject_rows` with `reason_code='duplicate_key'`
      - insert staging and rejects appropriately,
      - And update run `status` for `ingest_run` appropriately.

    Raises only on infra related exceptions (DB issues/bad connection, runtime errors, etc.).
    Will not raise on invalid data (will instead inject a `rejected_row` to table 'rejected_rows').
    """

    # collect appropriate table information.
    spec: TableSpec = get_table_spec(table_name)

    
    if spec.input_format == "csv":
        row_iter = stream_csv_dict_rows(input_path)
    else:
        row_iter = stream_jsonl_dict_rows(input_path)

    ## -- create run ledger, committed immediately
    run_id: UUID = insert_ingest_run(conn, input_path=input_path, table_name=table_name)
    conn.commit()

    total = loaded = rejected = 0

    work_batch: list[WorkRow] = []
    reject_batch: list[RejectInsert] = []       

    ## -- Begin transformations
    try:
        # Each work table is signal session-scoped. Create and truncate per run.
        prepare_work_table(conn, table_name=table_name)

        for source_row, raw in row_iter:
            total += 1

            ## -- Parse a row. Any exception here is a bug and should fail the run.
            res = spec.parser.parse(raw, source_row=source_row)

            ## -- Route each row to staging vs rejection.
            if hasattr(res, "to_mapping"):
                # stage validly parsed row (duplicates are not checked yet). 
                m = dict(cast(Any, res).to_mapping())
                # inject the lineage metadata required by a staging table if it needs it
                if "source_row" in TABLE_SPECS[table_name].columns:
                    m["source_row"] = source_row
                
                # append `raw_payload` and staging mapping as well
                work_batch.append(
                    WorkRow(
                        source_row=int(source_row),
                        raw_payload=dict(cast(Any, res).raw_payload),
                        staging_mapping=m,
                    )
                )
            else:
                rejected += 1
                # reject invalid row
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
            if len(work_batch) >= BATCH_SIZE:
                insert_work_rows(conn, table_name=table_name, run_id=run_id, rows=work_batch)
                work_batch.clear()

            if len(reject_batch) >= BATCH_SIZE:
                insert_reject_rows(conn, run_id=run_id, rejects=reject_batch)
                reject_batch.clear()

        ## -- flush any remainder left from last session
        insert_work_rows(conn, table_name=table_name, run_id=run_id, rows=work_batch)
        work_batch.clear()
        insert_reject_rows(conn, run_id=run_id, rejects=reject_batch)
        reject_batch.clear()


        # finalize rows before staging by deduping work
        inserted, dup_rejects = finalize_work_to_staging(conn, table_name=table_name, run_id=run_id)
        loaded += inserted
        rejected += dup_rejects


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

