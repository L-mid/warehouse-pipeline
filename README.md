# warehouse-pipeline
This is a Postgres staging ingest with rejects, data quality metrics, and exposing some explicit business SQL views. 

## This project demonstrates an example pipeline for: 
- A locally hosted Postgres database with saved tables.
- Typed ingestion (CSV/JSONL) and run lineage (`ingest_runs` table)
- Row-level rejection with explicit queryable reasons (`reject_rows` table)
- Data quality metrics are derived for inspection (`dq_results` table) (reruns for these metrics are idempotent as well)

- Extensible business outputs section with easy to access SQL views (currently contains: `daily_revenue`, `new_customers_by_day`)

- Additonally: Postgres is Dockerized + pytest integration and unit tests + CI


## Installation:

### Dependencies
Please ensure you have the following pre-setup before proceeding:
- Docker + Docker Compose
- Python 3.11+

### Installation instructions for the terminal:
```bash
python -m venv .venv

# Windows Powershell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

python -m pip install -U pip
pip install -e ".[dev]"

# Boots up your local docker and runs tests
make demo

# this initalizes the DB to you locally (do this ONLY once if you already haven't before):
pipeline db init        # restarts the database from scratch every time run
```


## Load up sample data to inspect some tables

Using the command `load`, input a file.

```bash
# ensures docker is up on a fresh container instance 
make down
make demo

# load up the sample data 
pipeline load --input data/sample/customers-1000.csv --table stg_customers
# and then:
pipeline load --input data/sample/retail_transactions.csv --table stg_retail_transactions
# and then:
pipeline load --input data/sample/orders.csv --table stg_orders
# and then: 
pipeline load --input data/sample/order_items.csv --table stg_order_items
    
```
Upon completion, a short printed results summary will display in the terminal for the data that was input.


### Inspect the data you just loaded
You can query data from the Postgres tables by running psql inside your docker container. 
```bash
# query first 10 days of revenue
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM daily_revenue ORDER BY day LIMIT 10;"

# query first 10 new customers
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT * FROM new_customers_by_day ORDER BY day LIMIT 10;"

# query reason codes per table from rejected rows
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT table_name, reason_code, COUNT(*) AS n FROM reject_rows GROUP BY 1,2 ORDER BY n DESC LIMIT 12;"

# query 12 derived health metrics per table name
docker compose exec -T db psql -U postgres -d warehouse -c "SELECT table_name, check_name, metric_name, metric_value, passed FROM dq_results ORDER BY created_at DESC LIMIT 12;"
```



## Other useful commands: 
```bash
# Inspect the DB with psql, anytime:
docker compose exec db psql -U postgres -d warehouse 
# \q to quit

# fully resets the DB to a fresh instance:
make down               
make demo
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

Additionally:
- Tests use their own DB connection: `WAREHOUSE_TEST_DSN`. 

- Tests such as `tests/integration/test_business_views.py` uses golden CSVs to assert **exact** rows in the views. Avoids regressions or weirdness with continuous updates.

- CI runs `make demo` on every push (runs unit + integration). 


