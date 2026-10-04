.PHONY: help install lint lint-local lint-fix test clean

PIP ?= $(PYTHON) -m pip
REPO_ROOT := $(abspath $(CURDIR)/..)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
RUFF := $(PYTHON) -m ruff
PYTEST := $(PYTHON) -m pytest
export PYTHONPATH := $(CURDIR)/src:$(CURDIR)/../maia/src:$(CURDIR)/../atlas/src:$(PYTHONPATH)

help:
	@echo "Pleiades MCP package commands:"
	@echo "  make install     - Install the mcp package (editable) + mcp SDK"
	@echo "  make lint        - Run ruff (readonly) on the package"
	@echo "  make lint-fix    - Auto-fix lint issues"
	@echo "  make test        - Run unit tests"
	@echo "  make clean       - Remove build artifacts"

install:
	$(PIP) install -e .

lint-local:
	$(RUFF) check src tests

lint:
	$(RUFF) check src tests
	$(RUFF) format --check src tests

lint-fix:
	$(RUFF) check --fix src tests
	$(RUFF) format src tests

test:
	$(PYTEST) tests/ -q

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
