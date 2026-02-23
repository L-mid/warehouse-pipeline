SQL in init is schema + constraints + indexes

`run_id`: globally unique each run without coordinating an integer sequence across environments. UUIDs are great for "pipeline run" identifiers.


Why this design was used:

ingest_runs = one row per ingest attempt
stg_* = typed rows produced by that run
reject_rows = bad rows + reasons produced by that run

Supports architecture for:
- Reruns without clobbering,
- Auditability (lineage)
- debugging (reject reasons + raw payload)
- cleanup (cascade deletes per run)

This schema is not the pipeline itself. It's the state store the pipeline (should) write to.


Python parsers will produce two streams:
- Good rows: already typed / normalized (or at least  safely castable)
- Reject rows: include raw payload + reason
- if valid: insert into stg_* with run_id
- if invalid: insert into reject_rows with run_id



docker compose exec -T db psql -U postgres -d warehouse -c  "SELECT run_id, started_at, table_name, input_path, status
 FROM ingest_runs
 WHERE run_id = '928321c0-28d8-4546-938b-720d1d50fffd';"