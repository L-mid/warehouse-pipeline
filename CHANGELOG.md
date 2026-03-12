# Changelog

## Unreleased
### Added
- New live mode test that broadly tests results from live http requests off dummy json.
- CI marker `non_ci` to avoid CI testing certain tests (such as the live http one).

### Fixed
- Live mode bug where a `NotImplementedError` was being raised on the path despite being implemented.
- Docker no longer errors hard on tests when unavailble, now skips via fixutres (finally).
- Linters, type checkers, and formatting no longer disabled and now required on pre-commit.
- Minor typing and line length fixes everywhere to make ruff and pyright checks finally pass.

## v0.3.0 - 2026-03-09
### Added
- Added an HTTP API extractor skeleton for DummyJson + tests.
- Separations of concerns into respective directories, better skeleton code.
- Added minimal happy-path stub tests across the new ingestion, orchestration, database, stage, DQ, and publish flows to serve as expansion points later.
- Added snapshot-based smoke fixtures for fast local and integration test runs.
- Moved inline transformation data structures into dedicated dataclasses under `warehouse_pipeline.transform`, and updated pipeline outputs to return clearer run summaries for testing and debugging.

### Fixed
- Directory structure now refactored (especially tests).
- `sql/` directory now builds tables for DummyJson adjacent fields, not mock data, with clearer file organization into `schema/`, `transform/`, and `publish/`.
- Separation of inline data structures into appropriate Dataclasses in `warehouse_pipeline/transform` (previously `warehouse`), and now returns a nice summary for tests.


## v0.2.0 - 2026-03-02
### Added
- Sample data `orders.csv` and `order_items.csv` to data pipeline + tests.
- Per run temp/working table added: enables duplicate rejection using a 'first seen wins' acceptance method.
- Working/temp table behavioural tests under `tests/unit/db`
- `warehouse` cmd and a post staging data transformation pipeline.
- New `.sql` views under `sql/extras` using tables created with the new transformation pipeline. Also added goldens tests (`integration/extras`) that assert exact rows for exact data to ensure correctness for this pipeline over updates.
- Two more extra SQL queries under `sql/extras`, one to demonstrate distinct functionality and other for an incorrect vs correct fanout, fanout
emphasized (a test showing the fanout principle + a `fanout_trap.md`).
### Fixed
- DB initalizing SQL statement riser now raises much more information on error
- Fields in data quality checks now derive from the same unified `TABLESPEC` as in the row parser.
- 'init `sql` else parse single file' bug was parsing above iterator instead of provided file in `main.py`.
- Fixed casing oversight where values could not be standardized to a specific casing, implementations to do so now exist in `RowParser` (default across all fields) and in `FieldSpec` (custom per value override).
- Fixed queries under `sql/extras` to new casing expectations.


## v0.1.0 - 2026-02-23
### Added
- repo skeleton for pipeline work + base testing structure.
- `daily_revenue` + `new_customers_by_day` views (shows latest succeeded run stuff)
- Golden-data integration tests for views
### Fixed
- DB connection issues on Windows with defaulting to pre-set global vars.
- Minor config expectation issues in the loader with `reject_reasons`.
- tons of minor glue code connection bootstrap repo development things that come from starting a new repo
