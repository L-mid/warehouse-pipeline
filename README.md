# warehouse-pipeline
This is a Postgres staging ingest with rejects, data quality metrics, and exposing some explicit business SQL views.

## This project demonstrates an example pipeline for:
- A Postgres-backed pipeline.
- Snapshot and live extract modes for Square orders.
- Typed mapping into `stg_square_*` tables in Postgres.
- Rejection capturing.
- Data quality metrics and gates.
- Warehouse transforms.
- Published SQL views over the current warehouse tables.

- Additonally: has pytest integration and unit tests that CI runs on every push.


## Installation:

### Dependencies
Please ensure you have the following pre-setup before proceeding:
- Docker + Docker Compose
- Python 3.11+

### Installation instructions:
```powershell
python -m venv .venv

# Windows Powershell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

python -m pip install -U pip
pip install -e ".[dev]"

```


## Run a snapshot pipeline execution
```powershell
# ensures docker is up on a fresh container instance
make down
make up
pipeline db init

# run the pipeline on a commited Square snapshot
pipeline run --mode snapshot --snapshot sandbox_v1

# You can also run live against the Square Sandbox API.
# Live mode requires Square Sandbox credentials. In the Square Developer Console,
# create an application, then copy its Sandbox Access Token and Sandbox location ID.
# Set them as `SQUARE_ACCESS_TOKEN` and `SQUARE_LOCATION_IDS` before running

$env:SQUARE_ACCESS_TOKEN="your-square-sandbox-token"
$env:SQUARE_LOCATION_IDS="your-location-id"

pipeline run --mode live
```
Upon completion, the CLI prints a run summary including the `run_id` and final status.


### Inspect the data you just ran
Some expamle inspection commands to see your data's in there:
```powershell
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT run_id, source_system, status, snapshot_key, started_at, finished_at FROM run_ledger ORDER BY started_at DESC LIMIT 5;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM stg_square_orders LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM fact_orders LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM fact_order_lines LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM fact_order_tenders LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_fact_orders_current LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_fact_order_lines_current LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_fact_order_tenders_current LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_dq_results_latest ORDER BY table_name, metric_name LIMIT 20;"
```

### Example business queries that were published
```powershell
Get-Content -Raw "sql\publish\metrics\010_daily_sales_summary.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\020_daily_sales_by_tender_type.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\030_weekly_top_items.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\040_daily_discount_summary.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\050_daily_order_state_summary.sql" | docker compose exec -T db psql -U postgres -d warehouse
```


## Other useful commands:
```powershell
# Inspect the DB with psql, anytime:
docker compose exec db psql -U postgres -d warehouse
# \q to quit

# fully resets the DB to a fresh instance:
make down
make up
pipeline db init


# run tests only
make test

```


## Tests

Pipeline tested with the sepearation of:

- Unit tests (run and work locally, fast)
- Integration tests (these require an active Postgres + docker instance)
- Heavy integration tests (slower larger tests)

Additionally:
- Tests use their own DB connection: `WAREHOUSE_TEST_DSN`.
- CI runs `make demo` on every push (runs unit + integration).
