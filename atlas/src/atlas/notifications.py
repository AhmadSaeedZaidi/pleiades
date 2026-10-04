import logging
from datetime import UTC, datetime
from enum import Enum

import aiohttp

from atlas.config import settings

logger = logging.getLogger("atlas.notifications")


class AlertLevel(Enum):
    INFO = 0x3498DB
    SUCCESS = 0x2ECC71
    WARNING = 0xF1C40F
    CRITICAL = 0xE74C3C


class AlertChannel(Enum):
    ALERTS = "alerts"
    HUNT = "hunt"
    SURVEILLANCE = "watch"
    OPS = "ops"


class DiscordNotifier:
    def __init__(self) -> None:
        self.env_tag = settings.ENV.upper()
        self.hooks = {
            AlertChannel.ALERTS: settings.DISCORD_WEBHOOK_ALERTS,
            AlertChannel.HUNT: settings.DISCORD_WEBHOOK_HUNT,
            AlertChannel.SURVEILLANCE: settings.DISCORD_WEBHOOK_SURVEILLANCE,
            AlertChannel.OPS: settings.DISCORD_WEBHOOK_OPS,
        }

    async def send(
        self,
        title: str,
        description: str,
        channel: AlertChannel = AlertChannel.ALERTS,
        level: AlertLevel = AlertLevel.INFO,
        fields: dict[str, str] | None = None,
    ) -> bool:
        """Post one Discord embed and report whether Discord accepted it."""
        secret = self.hooks.get(channel)

        if not secret:
            if channel != AlertChannel.ALERTS and self.hooks.get(AlertChannel.ALERTS):
                secret = self.hooks[AlertChannel.ALERTS]
            else:
                logger.warning(f"No webhook configured for channel {channel.value}")
                return False

        assert secret is not None, "Webhook secret should not be None at this point"
        webhook_url = secret.get_secret_value()

        embed = {
            "title": f"[{self.env_tag}] {title}",
            "description": description,
            "color": level.value,
            "timestamp": datetime.now(UTC).isoformat(),
            "footer": {"text": "Pleiades Atlas"},
            "fields": (
                [{"name": k, "value": str(v), "inline": True} for k, v in fields.items()]
                if fields
                else []
            ),
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(webhook_url, json={"embeds": [embed]}) as response:
                    if response.status not in [200, 204]:
                        logger.error(f"Discord alert failed: HTTP {response.status}")
                        return False
                    logger.info(f"Discord alert delivered to {channel.value}: {title}")
                    return True
            except Exception as e:
                logger.exception(f"Discord alert error: {e}")
                return False


notifier = DiscordNotifier()
