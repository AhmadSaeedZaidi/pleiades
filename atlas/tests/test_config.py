"""Tests for configuration module."""

import pytest
from pydantic import ValidationError


def test_settings_load(monkeypatch):
    """Settings load from environment and expose the expected defaults."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["test_key_1", "test_key_2"]')
    monkeypatch.setenv("VAULT_PROVIDER", "huggingface")
    monkeypatch.setenv("HF_DATASET_ID", "test/dataset")
    monkeypatch.setenv("HF_TOKEN", "hf_test_token")
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("COMPLIANCE_MODE", "false")

    from atlas.config import Settings

    settings = Settings()

    assert settings.ENV == "dev"
    # Default is now False; compliance mode must NOT collapse the key pool.
    assert settings.COMPLIANCE_MODE is False
    assert settings.VAULT_PROVIDER == "huggingface"


def test_api_keys_compliance_mode_preserves_rotation(monkeypatch):
    """Compliance mode must never collapse the key pool.

    The old behaviour truncated the pool to a single key on
    ``COMPLIANCE_MODE=true``, which silently disabled key rotation and
    guaranteed resiliency termination on the first quota error. Rotation and
    resilience must always be preserved.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["test_key_1", "test_key_2", "test_key_3"]')
    monkeypatch.setenv("VAULT_PROVIDER", "huggingface")
    monkeypatch.setenv("HF_DATASET_ID", "test/dataset")
    monkeypatch.setenv("HF_TOKEN", "hf_test_token")
    monkeypatch.setenv("COMPLIANCE_MODE", "true")

    from atlas.config import Settings

    settings = Settings()

    # All keys remain available for rotation even in compliance mode.
    keys = settings.api_keys
    assert len(keys) == 3
    assert keys[0] == "test_key_1"

    # Key rings are still split into multiple non-empty pools.
    rings = settings.key_rings
    assert set(rings) == {"hunting", "tracking", "archeology"}
    assert all(len(v) >= 1 for v in rings.values())


def test_api_keys_json_parsing(monkeypatch):
    """Test API key pool JSON parsing."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["key1", "key2", "key3"]')
    monkeypatch.setenv("VAULT_PROVIDER", "huggingface")
    monkeypatch.setenv("HF_DATASET_ID", "test/dataset")
    monkeypatch.setenv("HF_TOKEN", "hf_test_token")
    monkeypatch.setenv("COMPLIANCE_MODE", "false")

    from atlas.config import Settings

    settings = Settings()

    keys = settings.api_keys
    assert len(keys) == 3


@pytest.mark.parametrize(
    ("provider", "missing", "message"),
    [
        (
            "huggingface",
            {"HF_DATASET_ID": None, "HF_TOKEN": None},
            "HF_DATASET_ID and HF_TOKEN required",
        ),
        ("gcs", {"GCS_BUCKET_NAME": None}, "GCS_BUCKET_NAME required"),
    ],
)
def test_vault_validation_requires_provider_credentials(provider, missing, message):
    """Validate missing credentials without touching .env or starting a subprocess."""
    from atlas.config import Settings

    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, VAULT_PROVIDER=provider, **missing)
