# warehouse-pipeline
This is a Postgres staging ingest with rejects, data quality metrics, and exposing some explicit business SQL views. 

## This project demonstrates an example pipeline for: 
- A Postgres-backed pipeline.
- Snapshot and live extract modes from `https://dummyjson.com/`.
- Typed mapping into `stg_*` in Postgres.
- Rejection capturing.
- Data quality metrics and gates,
- Warehouse transforms.
- Published latest-run views.

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

# Boots up your local docker, runs a fast snapshot smoke pipeline, and runs tests.
make demo 
```


## Run a snapshot pipeline execution
```powershell
# ensures docker is up on a fresh container instance 
make down
make up
pipeline db init

# run the pipeline on a snapshot:
pipeline run --mode snapshot --snapshot v1
```
Upon completion, the CLI prints a run summary including the `run_id` and final status.


### Inspect the data you just ran
Some expamle inspection commands to see your data's in there:
```powershell
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT run_id, status, snapshot_key, started_at, finished_at FROM run_ledger ORDER BY started_at DESC LIMIT 5;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_fact_orders_latest LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_fact_order_items_latest LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_dim_customer_latest LIMIT 10;"
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM v_dq_results_latest ORDER BY table_name, metric_name LIMIT 20;"
```

### Example business queries that were published
```powershell
Get-Content -Raw "sql\publish\metrics\010_revenue_by_day_country.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\020_top_products_per_week.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\030_paid_vs_refunded_counts.sql" | docker compose exec -T db psql -U postgres -d warehouse
Get-Content -Raw "sql\publish\metrics\051_fanout_trap_right.sql" | docker compose exec -T db psql -U postgres -d warehouse
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

# run the 'inits and everything works' demo:
make demo
```


## Tests

Pipeline tested with the sepearation of:

- Unit tests (run and work locally, fast)
- Integration tests (these require an active Postgres + docker instance)
- Heavy integration tests (slower larger tests)

Additionally:
- Tests use their own DB connection: `WAREHOUSE_TEST_DSN`. 
- CI runs `make demo` on every push (runs unit + integration). 


