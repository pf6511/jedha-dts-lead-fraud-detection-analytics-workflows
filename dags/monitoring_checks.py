"""Decision rules of the pipeline checks.

The SQL queries measure, these functions decide: ok or alert. Kept
apart from the DAG so they can be tested without Airflow or database.

Each check returns a result with the columns of
monitoring.check_results: check_name, value, threshold, status, details.
"""

from typing import Any

# No new transaction for more than 15 minutes: the worker is stopped,
# blocked, or every payment fails
FRESHNESS_THRESHOLD_MINUTES = 15.0
STATUS_ALERT = "alert"

# More than 5 % of the payments of the last 24 hours quarantined: the
# source sends wrong data
QUARANTINE_RATE_THRESHOLD_PERCENT = 5.0

# Any failed payment waiting for a replay deserves attention
PENDING_FAILURES_THRESHOLD = 0


def freshness_result(
    minutes_since_last_transaction: float | None,
    threshold: float = FRESHNESS_THRESHOLD_MINUTES,
) -> dict[str, Any]:
    """Is the processor still storing transactions?"""
    if minutes_since_last_transaction is None:
        return {
            "check_name": "freshness",
            "value": None,
            "threshold": threshold,
            "status": STATUS_ALERT,
            "details": "no transaction stored yet",
        }

    minutes = float(minutes_since_last_transaction)

    return {
        "check_name": "freshness",
        "value": round(minutes, 1),
        "threshold": threshold,
        "status": STATUS_ALERT if minutes > threshold else "ok",
        "details": f"last transaction {minutes:.0f} minutes ago",
    }

def quarantine_rate_result(
    quarantined_count: int,
    stored_count: int,
    threshold: float = QUARANTINE_RATE_THRESHOLD_PERCENT,
) -> dict[str, Any]:
    """Share of the payments of the last 24 hours put in quarantine."""
    received = quarantined_count + stored_count

    # No payment at all: nothing to measure (the freshness check alerts)
    if received == 0:
        return {
            "check_name": "quarantine_rate",
            "value": None,
            "threshold": threshold,
            "status": "ok",
            "details": "no payment in the last 24 hours",
        }

    rate = quarantined_count / received * 100

    return {
        "check_name": "quarantine_rate",
        "value": round(rate, 2),
        "threshold": threshold,
        "status": STATUS_ALERT if rate > threshold else "ok",
        "details": (
            f"{quarantined_count} of {received} payments quarantined "
            f"in the last 24 hours ({rate:.1f} %)"
        ),
    }


def pending_failures_result(
    pending_count: int,
    oldest_pending_hours: float | None,
    threshold: int = PENDING_FAILURES_THRESHOLD,
) -> dict[str, Any]:
    """Failed payments waiting for a replay."""
    if pending_count <= threshold:
        return {
            "check_name": "pending_failures",
            "value": pending_count,
            "threshold": threshold,
            "status": "ok",
            "details": "no failed payment waiting for a replay",
        }

    return {
        "check_name": "pending_failures",
        "value": pending_count,
        "threshold": threshold,
        "status": STATUS_ALERT,
        "details": (
            f"{pending_count} failed payments waiting for a replay, "
            f"oldest {float(oldest_pending_hours):.1f} hours ago; "
            "run: uv run fraud-detection-replay"
        ),
    }
