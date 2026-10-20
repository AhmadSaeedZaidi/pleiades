"""Pytest configuration for live-only Alkyone preflight checks."""

import pytest
from alkyone.guard import assert_live_opt_in


def pytest_configure(config: pytest.Config) -> None:
    """Fail closed before collecting or importing any live test module."""
    del config
    assert_live_opt_in()
