REPO_ROOT := $(abspath $(CURDIR)/..)
PYTHON ?= $(if $(wildcard $(REPO_ROOT)/.venv/bin/python),$(REPO_ROOT)/.venv/bin/python,python)
export PYTHONPATH := $(CURDIR)/src:$(REPO_ROOT)/atlas/src:$(PYTHONPATH)

.PHONY: install lint lint-local test run demo
install:
	$(PYTHON) -m pip install -e .
lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m ruff format --check src tests
	$(PYTHON) -m mypy src --config-file pyproject.toml
	node --check src/pleiades_dashboard/static/app.js
	node --check src/pleiades_dashboard/static/graph.js
lint-local:
	$(PYTHON) -m ruff check --fix src tests
	$(PYTHON) -m ruff format src tests
test:
	$(PYTHON) -m pytest tests -q
run:
	cd $(REPO_ROOT) && PYTHONPATH=dashboard/src:atlas/src $(PYTHON) -m pleiades_dashboard
demo:
	cd $(REPO_ROOT) && PYTHONPATH=dashboard/src:atlas/src $(PYTHON) -m pleiades_dashboard --demo
