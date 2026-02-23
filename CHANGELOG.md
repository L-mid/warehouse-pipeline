# Changelog

## Unreleased

## v0.1.0 - 2026-02-23
### Added
- repo skeleton for pipeline work + base testing structure.
- `daily_revenue` + `new_customers_by_day` views (shows latest succeeded run)
- Golden-data integration tests for views
### Fixed
- DB connection issues on Windows with hijacking pre-set vars.
- Minor config expectation issues in the loader with 'reject_reasons'.