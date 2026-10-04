"""Discord notification delivery contract tests."""

from unittest.mock import MagicMock

import pytest
from atlas.notifications import AlertChannel, DiscordNotifier


class _Response:
    def __init__(self, status: int) -> None:
        self.status = status

    async def __aenter__(self) -> "_Response":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _Session:
    def __init__(self, status: int) -> None:
        self.status = status

    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def post(self, *args: object, **kwargs: object) -> _Response:
        return _Response(self.status)


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected"), [(204, True), (429, False)])
async def test_send_reports_discord_delivery(
    monkeypatch: pytest.MonkeyPatch, status: int, expected: bool
) -> None:
    notifier = DiscordNotifier()
    notifier.hooks[AlertChannel.ALERTS] = MagicMock(
        get_secret_value=MagicMock(return_value="https://discord.invalid/webhook")
    )

    def session(*, timeout):
        assert timeout.total == 10
        return _Session(status)

    monkeypatch.setattr("atlas.notifications.aiohttp.ClientSession", session)

    assert await notifier.send("Status", "Details") is expected


@pytest.mark.asyncio
async def test_send_reports_missing_webhook() -> None:
    notifier = DiscordNotifier()
    notifier.hooks = {channel: None for channel in AlertChannel}

    assert await notifier.send("Status", "Details", AlertChannel.OPS) is False


@pytest.mark.asyncio
async def test_webhook_errors_do_not_expose_private_url(monkeypatch, caplog) -> None:
    notifier = DiscordNotifier()
    notifier.hooks[AlertChannel.ALERTS] = MagicMock(
        get_secret_value=MagicMock(return_value="https://discord.invalid/private-token")
    )

    def fail_session(**kwargs):
        raise RuntimeError("https://discord.invalid/private-token")

    monkeypatch.setattr("atlas.notifications.aiohttp.ClientSession", fail_session)
    assert await notifier.send("Status", "Details") is False
    assert "RuntimeError" in caplog.text
    assert "private-token" not in caplog.text
