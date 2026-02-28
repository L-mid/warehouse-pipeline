# Changelog

## Unreleased
### Added
- Sample data `orders.csv` and `order_items.csv` to data pipeline + tests.
- Per run temp/working table added: enables duplicate rejection using a 'first seen wins' acceptance method.
- Working/temp table behavioural tests under `tests/unit/db`
- `warehouse` cmd and a post staging data transformation pipeline.
- New `.sql` views under `sql/extras` using tables created with the new transformation pipeline. Also added goldens tests (`integration/extras`) that assert exact rows for exact data to ensure correctness for this pipeline over updates.
### Fixed
- DB initalizing SQL statement riser now raises much more information on error
- Fields in data quality checks now derive from the same unified `TABLESPEC` as in the row parser.
- 'init `sql` else parse single file' bug was parsing above iterator instead of provided file in `main.py`.

## v0.1.0 - 2026-02-23
### Added
- repo skeleton for pipeline work + base testing structure.
- `daily_revenue` + `new_customers_by_day` views (shows latest succeeded run stuff)
- Golden-data integration tests for views
### Fixed
- DB connection issues on Windows with defaulting to pre-set global vars.
- Minor config expectation issues in the loader with `reject_reasons`.
- tons of minor glue code connection bootstrap repo development things that come from starting a new repo


