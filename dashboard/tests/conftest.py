import os

from unit_test_guard import isolate_unit_database as isolate_unit_database

# Atlas validates settings at import time; all API tests inject a fake reader.
os.environ.update(
    {
        "DATABASE_URL": "postgresql://test:test@127.0.0.1:1/test",
        "YOUTUBE_API_KEY_POOL_JSON": '["test-key"]',
        "VAULT_PROVIDER": "huggingface",
        "HF_DATASET_ID": "mock/dataset",
        "HF_TOKEN": "mock-token",
    }
)
