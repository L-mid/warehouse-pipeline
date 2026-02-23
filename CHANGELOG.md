# Changelog

## Unreleased
### Added
- None
### Fixed
- Hangs on a faulty connection now time out and error cleanly

## v0.1.0 - 2026-02-23
### Added
- repo skeleton for pipeline work + base testing structure.
- `daily_revenue` + `new_customers_by_day` views (shows latest succeeded run stuff)
- Golden-data integration tests for views
### Fixed
- DB connection issues on Windows with defaulting to pre-set global vars.
- Minor config expectation issues in the loader with `reject_reasons`.
- tons of minor glue code connection bootstrap repo development things that come from starting a new repo


