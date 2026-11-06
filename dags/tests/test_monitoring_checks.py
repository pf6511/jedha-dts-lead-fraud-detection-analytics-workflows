from monitoring_checks import (
    FRESHNESS_THRESHOLD_MINUTES,
    freshness_result,
    MIN_FRAUDS,
    pending_failures_result,
    precision_result,
    prevalence_result,
    quarantine_rate_result,
    recall_result,
    references_unavailable_result,
    STATUS_ALERT,
    STATUS_OK
)


def test_recent_transaction_is_ok():
    result = freshness_result(3.2)
    assert result["status"] == STATUS_OK
    assert result["check_name"] == "freshness"
    assert result["threshold"] == FRESHNESS_THRESHOLD_MINUTES


def test_old_transaction_is_an_alert():
    result = freshness_result(45)
    assert result["status"] == STATUS_ALERT
    assert "45 minutes" in result["details"]


def test_no_transaction_is_an_alert():
    result = freshness_result(None)
    assert result["status"] == STATUS_ALERT
    assert result["value"] is None


def test_low_quarantine_rate_is_ok():
    result = quarantine_rate_result(quarantined_count=2, stored_count=198)
    assert result["status"] == STATUS_OK
    assert result["value"] == 1.0


def test_high_quarantine_rate_is_an_alert():
    result = quarantine_rate_result(quarantined_count=20, stored_count=180)
    assert result["status"] == STATUS_ALERT
    assert result["value"] == 10.0


def test_no_payment_gives_no_quarantine_rate():
    result = quarantine_rate_result(quarantined_count=0, stored_count=0)
    assert result["status"] == STATUS_OK
    assert result["value"] is None


def test_no_pending_failure_is_ok():
    result = pending_failures_result(pending_count=0, oldest_pending_hours=None)
    assert result["status"] == STATUS_OK


def test_pending_failures_are_an_alert_with_the_replay_command():
    result = pending_failures_result(pending_count=3, oldest_pending_hours=5.25)
    assert result["status"] == STATUS_ALERT
    assert result["value"] == 3
    assert "fraud-detection-replay" in result["details"]


def test_good_recall_is_ok():
    result = recall_result(true_positive_count=30, actual_fraud_count=40, reference=0.72)
    assert result["status"] == STATUS_OK
    assert result["value"] == 0.75


def test_low_recall_is_an_alert():
    result = recall_result(true_positive_count=10, actual_fraud_count=40, reference=0.72)
    assert result["status"] == STATUS_ALERT


def test_recall_on_too_few_frauds_does_not_conclude():
    result = recall_result(true_positive_count=1, actual_fraud_count=MIN_FRAUDS - 1, reference=0.72)
    assert result["status"] == STATUS_OK
    assert "not enough" in result["details"]


def test_low_precision_is_an_alert():
    result = precision_result(true_positive_count=10, predicted_fraud_count=40, reference=0.84)
    assert result["status"] == STATUS_ALERT


def test_no_fraud_alert_gives_no_precision():
    result = precision_result(true_positive_count=0, predicted_fraud_count=0, reference=0.84)
    assert result["status"] == STATUS_OK
    assert result["value"] is None


def test_usual_prevalence_is_ok():
    result = prevalence_result(actual_fraud_count=8, evaluated_count=2000, reference=0.004)
    assert result["status"] == STATUS_OK


def test_doubled_prevalence_is_an_alert():
    result = prevalence_result(actual_fraud_count=20, evaluated_count=2000, reference=0.004)
    assert result["status"] == STATUS_ALERT


def test_prevalence_on_too_few_payments_does_not_conclude():
    result = prevalence_result(actual_fraud_count=5, evaluated_count=300, reference=0.004)
    assert result["status"] == STATUS_OK


def test_missing_references_are_an_alert():
    result = references_unavailable_result("recall_7d")
    assert result["status"] == STATUS_ALERT
    assert result["check_name"] == "recall_7d"
