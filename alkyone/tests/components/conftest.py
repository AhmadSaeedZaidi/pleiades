"""
Global pytest configuration for Alkyone.
Imports fixtures from src/alkyone/fixtures.py to make them available to all tests.

CRITICAL: This conftest does NOT mock Vault or DAO for integration tests.
Integration tests use REAL infrastructure (HuggingFace, Neon DB, YouTube API).

Environment variables are provided by:
- CI: GitHub Secrets and Variables (see .github/workflows/ci.yml)
- Local: Shell environment (export VAR=value)
- Atlas: Pydantic BaseSettings (automatically loads from environment)

DO NOT use dotenv here - Atlas handles environment loading.
"""

# CRITICAL: Override HF_DATASET_ID before ANY atlas.* import below, unless
# ``PLEIADES_USE_PRODUCTION_VAULT=1`` (E2E against the real vault from ``.env``).
# Mirrors alkyone.fixtures; duplicated because pytest may load conftest first.
import os as _os  # noqa: E402

if _os.getenv("PLEIADES_USE_PRODUCTION_VAULT", "").lower() not in ("1", "true", "yes"):
    _os.environ["HF_DATASET_ID"] = _os.getenv("HF_DATASET_ID_TEST", "Rolaficus/pleiades-vault-test")

from unittest.mock import AsyncMock, patch  # noqa: E402

import pytest  # noqa: E402


def pytest_configure(config):
    """
    Register custom markers for test categorization.

    This runs once at the start of the test session.
    """
    config.addinivalue_line(
        "markers", "integration: marks tests requiring REAL infrastructure (DB, Vault, API)"
    )
    config.addinivalue_line(
        "markers",
        "production_e2e: E2E run against the production HuggingFace vault; requires "
        "PLEIADES_USE_PRODUCTION_VAULT=1 (no automatic test-dataset override)",
    )

    # ── Prod-safety interlock ───────────────────────────────────────────────
    # Mirror the effective DATABASE_URL / HF_DATASET_ID from atlas settings into
    # os.environ (settings may have loaded them from .env), then refuse the run
    # if either resolves to the known production target. Runs before collection
    # so we never even import test code pointed at prod.
    try:
        from atlas.config import settings as _settings

        for _k in ("DATABASE_URL", "HF_DATASET_ID"):
            _v = getattr(_settings, _k, None)
            if _v is not None and not _os.getenv(_k):
                _os.environ[_k] = (
                    _v.get_secret_value() if hasattr(_v, "get_secret_value") else str(_v)
                )
    except Exception:
        pass

    from alkyone.guard import assert_isolated_test_environment

    assert_isolated_test_environment()


def pytest_collection_modifyitems(config, items):
    """
    SAFETY INTERLOCK: Skip integration tests if real infrastructure is unavailable.

    This prevents integration tests from running with mocked/dummy data, which would
    give false confidence. Integration tests either run against REAL services or don't run at all.

    Required for integration tests:
    - DATABASE_URL: Real Neon PostgreSQL connection
    - HF_TOKEN + HF_DATASET_ID: Real HuggingFace vault
    - YOUTUBE_API_KEY_POOL_JSON: Real YouTube API keys (not dummy/test keys)

    Configuration is loaded from ``atlas.config.settings`` (Pydantic BaseSettings),
    which respects both shell env vars (CI) and ``.env`` file (local dev).

    If ANY credential is missing or looks like a dummy, integration tests are SKIPPED.
    """
    missing: list[str] = []

    try:
        from atlas.config import settings

        if not settings.DATABASE_URL:
            missing.append("DATABASE_URL")

        if not settings.HF_TOKEN or not settings.HF_DATASET_ID:
            missing.append("HF_TOKEN/HF_DATASET_ID")

        api_keys = settings.api_keys
        if not api_keys:
            missing.append("YOUTUBE_API_KEY_POOL_JSON (no keys)")
        else:
            first = api_keys[0].upper()
            dummy_markers = ("DUMMY", "TEST_", "FAKE", "MOCK", "EXAMPLE")
            if any(m in first for m in dummy_markers) or len(api_keys[0]) < 30:
                missing.append("YOUTUBE_API_KEY_POOL_JSON (dummy keys)")
    except Exception as exc:
        missing.append(f"atlas.config load failure: {exc}")

    if missing:
        skip_msg = (
            f"Integration tests require REAL infrastructure. Missing: {', '.join(missing)}. "
            f"In CI: Check GitHub Secrets. Local: Set values in .env or shell env."
        )
        skip_integration = pytest.mark.skip(reason=skip_msg)

        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


@pytest.fixture
def mock_sleep():
    """Mock asyncio.sleep to speed up unit tests."""
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock:
        yield mock


# Import fixtures from alkyone.fixtures
