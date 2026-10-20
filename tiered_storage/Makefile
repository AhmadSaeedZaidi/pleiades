REPO_ROOT := $(abspath $(CURDIR)/..)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
export PYTHONPATH := $(CURDIR)/src:$(PYTHONPATH)
.DEFAULT_GOAL := help

.PHONY: help install lint lint-local test clean
help:
	@echo "make install | lint | test — standalone tiered storage package"
install:
	$(PYTHON) -m pip install -e .
lint:
	$(PYTHON) -m ruff check src tests examples
	$(PYTHON) -m ruff format --check src tests examples
	$(PYTHON) -m mypy src --config-file pyproject.toml
lint-local:
	$(PYTHON) -m ruff check --fix src tests
	$(PYTHON) -m ruff format src tests examples
test:
	$(PYTHON) -m pytest tests -q
clean:
	rm -rf build dist .pytest_cache .mypy_cache
