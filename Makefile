## Makefile to NOT prefix env vars inline

.PHONY: demo up down test

PY ?= python

up:
	docker compose up -d

down:
	docker compose down -v

test:
	$(PY) -m pytest -q

demo: up
	$(PY) scripts/demo.py
	$(PY) -m pytest -q

release:
	@test -n "$(VERSION)" || (echo "Usage: make release VERSION=v0.1.0" && exit 2)
	$(MAKE) demo
	$(PY) scripts/release.py --version "$(VERSION)"
	git add pyproject.toml CHANGELOG.md
	git commit -m "tag: release $(VERSION)"
	git tag "$(VERSION)"
	@echo "Tagged $(VERSION). Next: git push && git push --tags"
