.PHONY: help install lint lint-local test test-unit test-int test-live guard clean

REPO_ROOT := $(abspath $(CURDIR)/..)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
POETRY ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/poetry),$(REPO_ROOT)/.venv/bin/poetry,poetry)
POETRY_ENV := $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),VIRTUAL_ENV=$(REPO_ROOT)/.venv,)
RUFF := $(PYTHON) -m ruff
MYPY := $(PYTHON) -m mypy
PYTEST := $(PYTHON) -m pytest
export PYTHONPATH := $(CURDIR)/src:$(CURDIR)/../maia/src:$(CURDIR)/../atlas/src:$(PYTHONPATH)
export PYTHONPATH := $(CURDIR)/../tiered_storage/src:$(PYTHONPATH)
.DEFAULT_GOAL := help

.PHONY: test-storage
test-storage: guard
	$(PYTEST) tests/storage -v

help:
	@echo "Alkyone Development Commands:"
	@echo "  make install     - Install dependencies (editable mode)"
	@echo "  make lint        - Run linters (ruff, mypy)"
	@echo "  make test-unit   - Run Alkyone guard unit tests"
	@echo "  make test        - Alias for isolated test-int (explicit opt-in)"
	@echo "  make test-int    - Run isolated integration tests (explicit opt-in)"
	@echo "  make test-live   - Run explicitly opted-in live smoke tests"
	@echo "  make guard       - Isolated identity safety check (explicit opt-in)"
	@echo "  make clean       - Clean build artifacts"

install:
	$(POETRY_ENV) $(POETRY) install --with dev

lint-local:
	$(RUFF) check --fix src/alkyone tests
	$(RUFF) format src/alkyone tests
	$(MYPY) src/alkyone --config-file pyproject.toml --no-error-summary

lint:
	$(RUFF) check src/alkyone tests
	$(RUFF) format --check src/alkyone tests
	$(MYPY) src/alkyone --config-file pyproject.toml --no-error-summary

guard:
	$(PYTHON) -m alkyone.guard

test-unit:
	$(PYTEST) tests/test_guard.py -q

test-int:
	$(PYTHON) -m alkyone.guard
	$(PYTEST) tests/components -m integration -v --tb=short

test-live:
	$(PYTHON) -m alkyone.guard --live
	$(PYTEST) tests/live/test_preflight.py -m "live and smoke" -v --tb=short

test: test-int

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf dist build .pytest_cache .mypy_cache htmlcov
