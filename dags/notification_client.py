import logging
from typing import Any
import httpx

from fraud_detection_environment import Environment

environment = Environment()

DISCORD_FRAUD_WEBHOOK_URL = environment.env_var(
    "DISCORD_FRAUD_WEBHOOK_URL"
)

logger = logging.getLogger(__name__)


def notify_daily_metrics_generated(
    metrics: dict[str, Any],
) -> bool:
    """Notify Discord when the daily fraud report is ready."""

    message = "\n".join(
        [
            "**DAily fraud detection report ready**",
            f"**Report date :** `{metrics['report_date']}`",
            f"**Period start :** `{metrics['period_start']}`",
            f"**Period end :** `{metrics['period_end']}`",
            f"**Evaluated transactions :** "
            f"`{metrics['evaluated_transaction_count']}`",
            f"**Predicted frauds :** "
            f"`{metrics['predicted_fraud_count']}`",
        ]
    )

    payload = {
        "content": message,
        "allowed_mentions": {"parse": []},
    }

    try:

        response = httpx.post(
            DISCORD_FRAUD_WEBHOOK_URL,
            json=payload,
            timeout=10.0,
        )
        response.raise_for_status()
        return True

    except httpx.HTTPStatusError as exc:
            logger.error(
                "Discord notification rejected: HTTP %s - %s",
                exc.response.status_code,
                exc.response.text[:500],
            )
            return False

    except httpx.HTTPError as exc:
            logger.error(
                "Discord notification failed (%s)",
                type(exc).__name__,
            )
            return False

    except RuntimeError as exc:
            logger.error(
                "Discord notification configuration error: %s",
                str(exc),
            )
            return False