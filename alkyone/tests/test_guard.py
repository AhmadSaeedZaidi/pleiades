"""Unit tests for Alkyone's fail-closed environment guard."""

import pytest

from alkyone import guard


def _set_isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Populate a complete, non-production isolated test environment."""
    monkeypatch.delenv("YOUTUBE_API_KEY_POOL_JSON", raising=False)
    values = {
        "ALKYONE_ALLOW_INTEGRATION": "1",
        "DATABASE_URL": "postgresql://test:test@test-db/pleiades_test",
        "ALKYONE_TEST_DATABASE_URL": "postgresql://test:test@test-db/pleiades_test",
        "HF_DATASET_ID": "example/pleiades-test-vault",
        "ALKYONE_TEST_VAULT": "example/pleiades-test-vault",
        "PLEIADES_PROD_DATABASE_URL": "postgresql://prod-db/pleiades",
        "PLEIADES_PROD_VAULT": "example/pleiades-vault",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_isolated_environment_requires_positive_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("ALKYONE_ALLOW_INTEGRATION")

    with pytest.raises(SystemExit, match="ALKYONE_ALLOW_INTEGRATION=1"):
        guard.assert_isolated_test_environment()


def test_isolated_environment_requires_test_identities(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("ALKYONE_TEST_DATABASE_URL")

    with pytest.raises(SystemExit, match="isolated test identity"):
        guard.assert_isolated_test_environment()


def test_isolated_environment_requires_production_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("PLEIADES_PROD_VAULT")

    with pytest.raises(SystemExit, match="PLEIADES_PROD_VAULT"):
        guard.assert_isolated_test_environment()


def test_isolated_environment_accepts_matching_nonproduction_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)

    guard.assert_isolated_test_environment()


@pytest.mark.parametrize("variable", ["DATABASE_URL", "HF_DATASET_ID"])
def test_isolated_environment_rejects_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.setenv(variable, "https://unexpected.example/production")

    with pytest.raises(SystemExit, match="does not match"):
        guard.assert_isolated_test_environment()


def test_live_environment_requires_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALKYONE_ALLOW_LIVE", raising=False)

    with pytest.raises(SystemExit, match="ALKYONE_ALLOW_LIVE=1"):
        guard.assert_live_opt_in()


def test_live_environment_requires_production_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALKYONE_ALLOW_LIVE", "1")
    monkeypatch.setenv("DATABASE_URL", "postgresql://prod-db/pleiades")
    monkeypatch.setenv("HF_DATASET_ID", "example/pleiades-vault")

    with pytest.raises(SystemExit, match="identity"):
        guard.assert_live_opt_in()


def test_live_environment_accepts_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALKYONE_ALLOW_LIVE", "1")
    monkeypatch.setenv("DATABASE_URL", "postgresql://prod-db/pleiades")
    monkeypatch.setenv("HF_DATASET_ID", "example/pleiades-vault")
    monkeypatch.setenv("PLEIADES_PROD_DATABASE_URL", "postgresql://prod-db/pleiades")
    monkeypatch.setenv("PLEIADES_PROD_VAULT", "example/pleiades-vault")

    guard.assert_live_opt_in()


def test_database_identity_comparison_preserves_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://TEST:test@test-db/pleiades_test")

    with pytest.raises(SystemExit, match="does not match"):
        guard.assert_isolated_test_environment()


def test_production_refusal_does_not_disclose_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_db = "postgresql://user:super-secret@prod-db/pleiades"
    monkeypatch.setenv("DATABASE_URL", secret_db)
    monkeypatch.setenv("PLEIADES_PROD_DATABASE_URL", secret_db)

    with pytest.raises(SystemExit) as refusal:
        guard.assert_not_production()

    assert "super-secret" not in str(refusal.value)


def test_not_production_fails_closed_when_prod_database_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: unsetting the prod reference must not mean "not production".

    assert_not_production() is what the fresh_db fixture calls immediately before
    TRUNCATE-ing every table in the public schema. Treating an empty
    PLEIADES_PROD_* as "not production" made deleting the variable a silent
    bypass of the only thing standing between a misconfigured run and total data
    loss.
    """
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("PLEIADES_PROD_DATABASE_URL")

    with pytest.raises(SystemExit, match="PLEIADES_PROD_DATABASE_URL is not set"):
        guard.assert_not_production()


def test_not_production_fails_closed_when_prod_vault_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("PLEIADES_PROD_VAULT")

    with pytest.raises(SystemExit, match="PLEIADES_PROD_VAULT is not set"):
        guard.assert_not_production()


def test_not_production_accepts_a_fully_declared_isolated_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.delenv("YOUTUBE_API_KEY_POOL_JSON", raising=False)

    guard.assert_not_production()


def test_not_production_refuses_the_shared_youtube_key_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prod key pool is a production target too: tests would burn live quota."""
    _set_isolated_environment(monkeypatch)
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["AIza-prod-key"]')
    monkeypatch.setenv("PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON", '["AIza-prod-key"]')

    with pytest.raises(SystemExit, match="PRODUCTION key pool"):
        guard.assert_not_production()


def test_not_production_fails_closed_on_undeclared_youtube_key_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A key pool with no production reference cannot be cleared, so it refuses."""
    _set_isolated_environment(monkeypatch)
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["AIza-something"]')
    monkeypatch.delenv("PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON", raising=False)

    with pytest.raises(SystemExit, match="PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON is not"):
        guard.assert_not_production()


def test_not_production_allows_a_dedicated_youtube_key_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_isolated_environment(monkeypatch)
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["AIza-test-key"]')
    monkeypatch.setenv("PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON", '["AIza-prod-key"]')

    guard.assert_not_production()
