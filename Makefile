## Makefile to NOT prefix env vars inline

.PHONY: demo up down test

PY ?= python

up:
	docker compose up -d --wait

down:
	docker compose down -v

init:
	pipeline db init

demo:
	$(MAKE) down
	$(MAKE) up
	pipeline db init
	pipeline run --mode snapshot --snapshot smoke --with-dq --with-warehouse

test:
	$(PY) -m pytest -q


release:
	@$(PY) -c "import sys; v=sys.argv[1]; \
print('Usage: make release VERSION=v0.1.0') or sys.exit(2) if not v else None" "$(VERSION)"
	$(MAKE) demo
	$(PY) scripts/release.py --version "$(VERSION)"
	git add pyproject.toml CHANGELOG.md
	git commit -m "tag: release $(VERSION)"
	git tag "$(VERSION)"
	@echo "Tagged $(VERSION). Next: git push && git push --tags"



## formatting and extras

PY := python

fmt:
	ruff check --fix .
	ruff format .

lint:
	ruff check .

format-check:
	ruff format --check .

typecheck:
	pyright

test:
	pytest

test-ci:
	CI=true pytest -m "not non_ci"

ci:
	$(PY) scripts/ci_gate.py

install-hooks:
	pre-commit install --hook-type pre-commit --hook-type pre-push
