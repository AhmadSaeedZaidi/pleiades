.PHONY: help install lint test test-unit clean

REPO_ROOT := $(abspath $(CURDIR)/..)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
POETRY ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/poetry),$(REPO_ROOT)/.venv/bin/poetry,poetry)
POETRY_ENV := $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),VIRTUAL_ENV=$(REPO_ROOT)/.venv,)
RUFF := $(PYTHON) -m ruff
MYPY := $(PYTHON) -m mypy
PYTEST := $(PYTHON) -m pytest
export PYTHONPATH := $(CURDIR)/src:$(CURDIR)/../atlas/src:$(PYTHONPATH)

help:
	@echo "Maia Development Commands:"
	@echo "  make install     - Install dependencies (editable mode)"
	@echo "  make lint        - Run Ruff, formatting, and mypy"
	@echo "  make test        - Run unit tests"
	@echo "  make test-unit   - Run unit tests (alias for test)"
	@echo "  make clean       - Clean build artifacts"

install:
	$(POETRY_ENV) $(POETRY) install --with dev

lint-local:
	$(RUFF) check --fix src/maia tests
	$(RUFF) format src/maia tests
	$(MYPY) src/maia --config-file pyproject.toml --no-error-summary

lint:
	$(RUFF) check src/maia tests
	$(RUFF) format --check src/maia tests
	$(MYPY) src/maia --config-file pyproject.toml --no-error-summary

test:
	$(PYTEST) tests -v

test-unit: test

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf dist build .pytest_cache .mypy_cache htmlcov
