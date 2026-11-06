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
STATUS_OK = "ok"

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
        "status": STATUS_ALERT if minutes > threshold else STATUS_OK,
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
            "status": STATUS_OK,
            "details": "no payment in the last 24 hours",
        }

    rate = quarantined_count / received * 100

    return {
        "check_name": "quarantine_rate",
        "value": round(rate, 2),
        "threshold": threshold,
        "status": STATUS_ALERT if rate > threshold else STATUS_OK,
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
            "status": STATUS_OK,
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

# ---------------------------------------------------------------------
# Model checks, over the last 7 days
# ---------------------------------------------------------------------

# The reference values (validation recall and precision, training
# prevalence p0) come from the MLflow run of the production model
# (mlflow_references_client) and are passed to each check.

# Alert when a metric falls under 75 % of its validation value
ALERT_RATIO = 0.75

# Below these volumes, a rate varies too much by chance to conclude
MIN_FRAUDS = 20
MIN_EVALUATED = 1000


def recall_result(
    true_positive_count: int,
    actual_fraud_count: int,
    reference: float,
) -> dict[str, Any]:
    """Share of the real frauds that the model detected."""
    threshold = round(reference * ALERT_RATIO, 4)
    recall = float(true_positive_count / actual_fraud_count) if actual_fraud_count else None

    if (recall is None or actual_fraud_count < MIN_FRAUDS):
        status = STATUS_OK
        details = f"not enough real frauds to conclude ({actual_fraud_count}, minimum {MIN_FRAUDS})"
    else:
        status = STATUS_ALERT if recall < threshold else STATUS_OK
        details = (
            f"{true_positive_count} of {actual_fraud_count} real frauds detected "
            f"(validation recall {reference})"
        )

    return {
        "check_name": "recall_7d",
        "value": round(recall, 4) if recall is not None else None,
        "threshold": threshold,
        "status": status,
        "details": details,
    }


def precision_result(
    true_positive_count: int,
    predicted_fraud_count: int,
    reference: float,
) -> dict[str, Any]:
    """Share of the fraud alerts that were real frauds."""
    threshold = round(reference * ALERT_RATIO, 4)
    precision = (
        true_positive_count / predicted_fraud_count if predicted_fraud_count else None
    )

    if (precision is None or predicted_fraud_count < MIN_FRAUDS):
        status = STATUS_OK
        details = f"not enough fraud alerts to conclude ({predicted_fraud_count}, minimum {MIN_FRAUDS})"
    else:
        status = STATUS_ALERT if precision < threshold else STATUS_OK
        details = (
            f"{true_positive_count} of {predicted_fraud_count} fraud alerts were real frauds "
            f"(validation precision {reference})"
        )

    return {
        "check_name": "precision_7d",
        "value": round(precision, 4) if precision is not None else None,
        "threshold": threshold,
        "status": status,
        "details": details,
    }


def prevalence_result(
    actual_fraud_count: int,
    evaluated_count: int,
    reference: float,
) -> dict[str, Any]:
    """Share of real frauds; alert when it is half or double the training one."""
    prevalence = actual_fraud_count / evaluated_count if evaluated_count else None

    if (prevalence is None or evaluated_count < MIN_EVALUATED):
        status = STATUS_OK
        details = f"not enough payments to conclude ({evaluated_count}, minimum {MIN_EVALUATED})"
    else:
        outside = prevalence < reference / 2 or prevalence > reference * 2
        status = STATUS_ALERT if outside else STATUS_OK
        details = (
            f"{actual_fraud_count} real frauds in {evaluated_count} payments "
            f"({prevalence:.2%}, training {reference:.2%}, alert outside half to double)"
        )

    return {
        "check_name": "prevalence_7d",
        "value": round(prevalence, 5) if prevalence is not None else None,
        "threshold": round(reference,5),
        "status": status,
        "details": details,
    }


def references_unavailable_result(check_name: str) -> dict[str, Any]:
    """Model check that cannot run: the MLflow references could not be read."""
    return {
        "check_name": check_name,
        "value": None,
        "threshold": None,
        "status": "alert",
        "details": "reference values of the production model unavailable (MLflow)",
    }
