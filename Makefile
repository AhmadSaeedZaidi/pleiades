.PHONY: help install check lint lint-fix test test-unit test-int test-live clean pre-commit-install pre-commit

REPO_ROOT := $(CURDIR)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
export PYTHON
export PYTHONPATH := $(REPO_ROOT)/tiered_storage/src:$(PYTHONPATH)

help:
	@echo "Pleiades Monorepo Development Commands:"
	@echo "  make install            - Install all dependencies"
	@echo "  make pre-commit-install - Install pre-commit hook scripts"
	@echo "  make pre-commit         - Run pre-commit on all files (commit stage)"
	@echo "  make lint               - Run linters (readonly) on all modules"
	@echo "  make lint-fix           - Auto-fix lint issues (ruff check --fix + format)"
	@echo "  make check              - Run readonly quality checks and hermetic unit tests"
	@echo "  make test               - Alias for check (hermetic)"
	@echo "  make test-unit          - Run hermetic Atlas + Maia + MCP + Alkyone guard tests"
	@echo "  make test-int           - Run isolated Alkyone integration tests"
	@echo "  make test-live          - Run explicitly opted-in live Alkyone smoke tests"
	@echo "  make dashboard          - Start read-only operator UI on localhost:8080"
	@echo "  make dashboard-demo     - Start UI with labeled sample data"
	@echo "  make clean              - Clean all artifacts"

install:
	$(MAKE) -C tiered_storage install
	$(MAKE) -C atlas install
	$(MAKE) -C maia install
	$(MAKE) -C alkyone install
	$(MAKE) -C mcp install
	$(MAKE) -C dashboard install

lint-local:
	$(MAKE) -C tiered_storage lint-local
	$(MAKE) -C atlas lint-local
	$(MAKE) -C maia lint-local
	$(MAKE) -C alkyone lint-local
	$(MAKE) -C mcp lint-local
	$(MAKE) -C dashboard lint-local

lint:
	$(MAKE) -C tiered_storage lint
	$(MAKE) -C atlas lint
	$(MAKE) -C maia lint
	$(MAKE) -C alkyone lint
	$(MAKE) -C mcp lint
	$(MAKE) -C dashboard lint
	$(PYTHON) -m ruff check conftest.py unit_test_guard.py tools/check_docs.py

pre-commit-install:
	pre-commit install --hook-type pre-commit --hook-type pre-push

pre-commit:
	pre-commit run --all-files

lint-fix:
	$(MAKE) -C tiered_storage lint-local
	$(MAKE) -C atlas lint-local
	$(MAKE) -C maia lint-local
	$(MAKE) -C alkyone lint-local
	$(MAKE) -C mcp lint-local
	$(MAKE) -C dashboard lint-local

check: lint test-unit docs-check

test: check

test-unit:
	$(MAKE) -C tiered_storage test
	$(MAKE) -C atlas test-unit
	$(MAKE) -C maia test-unit
	$(MAKE) -C mcp test
	$(MAKE) -C alkyone test-unit
	$(MAKE) -C dashboard test

.PHONY: dashboard dashboard-demo docs-check
dashboard:
	$(MAKE) -C dashboard run

dashboard-demo:
	$(MAKE) -C dashboard demo

docs-check:
	$(PYTHON) tools/check_docs.py


test-int:
	$(MAKE) -C alkyone test-int

test-live:
	$(MAKE) -C alkyone test-live

clean:
	$(MAKE) -C tiered_storage clean
	$(MAKE) -C atlas clean
	$(MAKE) -C maia clean
	$(MAKE) -C alkyone clean
	$(MAKE) -C mcp clean
