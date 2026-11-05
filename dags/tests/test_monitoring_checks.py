from monitoring_checks import (
    FRESHNESS_THRESHOLD_MINUTES,
    freshness_result,
    pending_failures_result,
    quarantine_rate_result,
)


def test_recent_transaction_is_ok():
    result = freshness_result(3.2)
    assert result["status"] == "ok"
    assert result["check_name"] == "freshness"
    assert result["threshold"] == FRESHNESS_THRESHOLD_MINUTES


def test_old_transaction_is_an_alert():
    result = freshness_result(45)
    assert result["status"] == "alert"
    assert "45 minutes" in result["details"]


def test_no_transaction_is_an_alert():
    result = freshness_result(None)
    assert result["status"] == "alert"
    assert result["value"] is None


def test_low_quarantine_rate_is_ok():
    result = quarantine_rate_result(quarantined_count=2, stored_count=198)
    assert result["status"] == "ok"
    assert result["value"] == 1.0


def test_high_quarantine_rate_is_an_alert():
    result = quarantine_rate_result(quarantined_count=20, stored_count=180)
    assert result["status"] == "alert"
    assert result["value"] == 10.0


def test_no_payment_gives_no_quarantine_rate():
    result = quarantine_rate_result(quarantined_count=0, stored_count=0)
    assert result["status"] == "ok"
    assert result["value"] is None


def test_no_pending_failure_is_ok():
    result = pending_failures_result(pending_count=0, oldest_pending_hours=None)
    assert result["status"] == "ok"


def test_pending_failures_are_an_alert_with_the_replay_command():
    result = pending_failures_result(pending_count=3, oldest_pending_hours=5.25)
    assert result["status"] == "alert"
    assert result["value"] == 3
    assert "fraud-detection-replay" in result["details"]
