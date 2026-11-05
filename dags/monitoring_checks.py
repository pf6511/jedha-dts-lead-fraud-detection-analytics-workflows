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
