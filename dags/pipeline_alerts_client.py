"""Discord alerts about the pipeline health (channel #pipeline-alerts).

Separate from notification_client (daily business report): technical
alerts and business reports go to different channels.

The webhook is read when an alert is sent, not when the module is
imported: if it is missing, only the alert fails, the DAGs still load.
"""

import logging
from typing import Any

import httpx

from fraud_detection_environment import Environment

logger = logging.getLogger(__name__)


def notify_pipeline_alerts(alerts: list[dict[str, Any]]) -> bool:
    """Send the failed checks to the #pipeline-alerts channel."""
    lines = ["**Fraud detection pipeline: alert**"]
    lines += [
        f"- **{alert['check_name']}**: {alert['details']} "
        f"(threshold {alert['threshold']})"
        for alert in alerts
    ]

    payload = {
        "content": "\n".join(lines),
        "allowed_mentions": {"parse": []},
    }

    try:
        webhook_url = Environment().env_var("DISCORD_PIPELINE_WEBHOOK_URL")

        response = httpx.post(webhook_url, json=payload, timeout=10.0)
        response.raise_for_status()
        return True

    except RuntimeError as exc:
        logger.error("Pipeline alert configuration error: %s", exc)
        return False

    except httpx.HTTPError as exc:
        logger.error("Pipeline alert failed (%s)", type(exc).__name__)
        return False
