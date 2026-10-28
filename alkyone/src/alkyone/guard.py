"""Safety interlock for Alkyone integration runs.

Alkyone talks to REAL infrastructure (a Postgres database and a HuggingFace
vault). To make sure it can never silently mutate production data, every
Alkyone session starts by calling :func:`assert_not_production`, which
**hard-refuses** (``SystemExit``) the run when the configured ``DATABASE_URL``
or ``HF_DATASET_ID`` matches the known production target.

Isolated runs additionally require an explicit opt-in and positive identity
checks. The active endpoints must match the test endpoints supplied via:

* ``ALKYONE_ALLOW_INTEGRATION=1`` -- explicit isolated-run opt-in
* ``ALKYONE_TEST_DATABASE_URL`` -- isolated Postgres connection string
* ``ALKYONE_TEST_VAULT``        -- isolated HuggingFace dataset repo id

Production targets are also supplied out-of-band (never committed) via:

* ``PLEIADES_PROD_DATABASE_URL`` -- the live Postgres connection string
* ``PLEIADES_PROD_VAULT``        -- the live HuggingFace dataset repo id

Set either to the real production value in your environment / CI secrets and
Alkyone will refuse to start if it is pointed at them. This is intentionally a
hard stop (not a skip): running integration tests against production is a
data-integrity incident, not a skipped test.

Every check here is FAIL-CLOSED. A missing production reference is a refusal,
not a pass: an interlock that can be switched off by unsetting an environment
variable is not an interlock. ``assert_not_production`` is additionally safe to
call on its own (the ``fresh_db`` fixture does, immediately before truncating
every table in the public schema), so it performs the full set of checks rather
than assuming a stronger guard already ran.
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger("alkyone.guard")


def _norm(value: str | None) -> str:
    return (value or "").strip().rstrip("/")


def _matches(prod: str | None, candidate: str | None) -> bool:
    prod_n, cand_n = _norm(prod), _norm(candidate)
    if not prod_n or not cand_n:
        return False
    return cand_n == prod_n


def _enabled(name: str) -> bool:
    """Return whether an explicit boolean environment flag is enabled."""
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _require_identity(name: str, candidate: str | None, expected: str | None) -> None:
    """Refuse when a required active endpoint is absent or not test identity."""
    if not _norm(expected):
        _refuse(f"{name} is not configured; provide the isolated test identity.")
    if not _norm(candidate):
        _refuse(f"{name} is not configured; isolated tests cannot start.")
    if not _matches(expected, candidate):
        _refuse(f"{name} does not match its isolated test identity.")


def assert_isolated_test_environment() -> None:
    """Require explicit opt-in and verified isolated DB/vault identities."""
    if not _enabled("ALKYONE_ALLOW_INTEGRATION"):
        _refuse(
            "isolated integration tests require ALKYONE_ALLOW_INTEGRATION=1. "
            "This opt-in is never enabled implicitly."
        )
    _require_identity(
        "DATABASE_URL", os.getenv("DATABASE_URL"), os.getenv("ALKYONE_TEST_DATABASE_URL")
    )
    _require_identity("HF_DATASET_ID", os.getenv("HF_DATASET_ID"), os.getenv("ALKYONE_TEST_VAULT"))
    if not _norm(os.getenv("PLEIADES_PROD_DATABASE_URL")):
        _refuse("PLEIADES_PROD_DATABASE_URL must identify the production database.")
    if not _norm(os.getenv("PLEIADES_PROD_VAULT")):
        _refuse("PLEIADES_PROD_VAULT must identify the production vault.")
    assert_not_production()
    logger.info("Alkyone guard: explicit isolated test identity verified.")


def assert_live_opt_in() -> None:
    """Require an explicit opt-in before any live smoke test selection."""
    if not _enabled("ALKYONE_ALLOW_LIVE"):
        _refuse(
            "live smoke tests require ALKYONE_ALLOW_LIVE=1. "
            "Use this only for an intentional production-connected check."
        )

    _require_identity(
        "DATABASE_URL",
        os.getenv("DATABASE_URL"),
        os.getenv("PLEIADES_PROD_DATABASE_URL"),
    )
    _require_identity(
        "HF_DATASET_ID",
        os.getenv("HF_DATASET_ID"),
        os.getenv("PLEIADES_PROD_VAULT"),
    )
    logger.warning("Alkyone live guard: explicit opt-in and production identities verified.")


def assert_not_production() -> None:
    """Raise ``SystemExit`` if Alkyone is pointed at production infrastructure.

    Called at session start (conftest ``pytest_configure`` + ``make guard``) and
    again by the ``fresh_db`` fixture before it truncates anything, so it must be
    safe to call on its own — it cannot assume a stronger guard already ran.

    This function is FAIL-CLOSED. An unset ``PLEIADES_PROD_*`` variable used to
    mean "not production", which made unsetting the variable a silent bypass of
    the interlock protecting ``reset_for_test()`` — a ``TRUNCATE`` of every table
    in the public schema. A guard that can be disabled by removing a variable is
    not a guard, so a missing production reference is now a refusal.
    """
    prod_db = os.getenv("PLEIADES_PROD_DATABASE_URL")
    db = os.getenv("DATABASE_URL")
    if not _norm(prod_db):
        _refuse(
            "PLEIADES_PROD_DATABASE_URL is not set, so the production database "
            "cannot be ruled out. Declare it to let Alkyone verify isolation."
        )
    if _matches(prod_db, db):
        _refuse(
            "DATABASE_URL matches the PRODUCTION database. "
            "Alkyone must run against an isolated test DB. "
            "Point DATABASE_URL at the declared isolated test instance."
        )

    prod_vault = os.getenv("PLEIADES_PROD_VAULT")
    vault = os.getenv("HF_DATASET_ID")
    if not _norm(prod_vault):
        _refuse(
            "PLEIADES_PROD_VAULT is not set, so the production vault cannot be "
            "ruled out. Declare it to let Alkyone verify isolation."
        )
    if _matches(prod_vault, vault):
        _refuse(
            "HF_DATASET_ID matches the PRODUCTION vault. "
            "Alkyone must run against an isolated test vault. "
            "Point HF_DATASET_ID at the declared isolated test vault."
        )

    _assert_no_production_youtube_credentials()
    logger.info("Alkyone guard: DATABASE_URL / HF_DATASET_ID are NOT production. Proceeding.")


def _assert_no_production_youtube_credentials() -> None:
    """Refuse when the run is wired to production-only external services.

    The DB and vault interlocks above do not cover the YouTube API key pool or
    the authenticated session cookie jar. Pointing an integration run at the
    production key pool silently burns the live daily quota, and several Alkyone
    tests call the real Data API. Both are production targets, so they follow the
    same explicit-reference, fail-closed rule: declare the production value, and
    a mismatch is a refusal.
    """
    pool = os.getenv("YOUTUBE_API_KEY_POOL_JSON")
    if _norm(pool):
        prod_pool = os.getenv("PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON")
        if not _norm(prod_pool):
            _refuse(
                "YOUTUBE_API_KEY_POOL_JSON is set but PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON "
                "is not, so the production key pool cannot be ruled out. Declare it, or "
                "unset YOUTUBE_API_KEY_POOL_JSON to run without live-API tests."
            )
        if _matches(prod_pool, pool):
            _refuse(
                "YOUTUBE_API_KEY_POOL_JSON is the PRODUCTION key pool. "
                "Alkyone must run against a dedicated test key pool so the live "
                "daily quota is not consumed."
            )

    cookies = os.getenv("YOUTUBE_COOKIES_PATH")
    if cookies and os.path.exists(cookies):
        logger.warning(
            "Alkyone guard: YOUTUBE_COOKIES_PATH points at a real cookie jar (%s). "
            "Integration tests will present it to YouTube.",
            cookies,
        )


def _refuse(reason: str) -> None:
    msg = f"ALKYONE REFUSAL: {reason}"
    logger.error(msg)
    sys.exit(f"\n{msg}\n")


if __name__ == "__main__":
    if "--live" in sys.argv[1:]:
        assert_live_opt_in()
    else:
        assert_isolated_test_environment()
