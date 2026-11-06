import os

import pytest


def test_notify_daily_metrics_generated_sends_to_discord() -> None:
    if os.getenv("RUN_DISCORD_INTEGRATION_TESTS") != "1":
        pytest.skip(
            "Set RUN_DISCORD_INTEGRATION_TESTS=1 to send "
            "a real Discord notification."
        )

    from notification_client import (
        notify_daily_metrics_generated,
    )

    metrics = {
        "report_date": "2099-01-01",
        "period_start": "2098-12-31T22:00:00+00:00",
        "period_end": "2099-01-01T22:00:00+00:00",
        "evaluated_transaction_count": 42,
        "predicted_fraud_count": 3,
    }

    notification_sent = notify_daily_metrics_generated(metrics)

    assert notification_sent is True