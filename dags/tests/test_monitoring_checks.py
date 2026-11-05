from monitoring_checks import FRESHNESS_THRESHOLD_MINUTES, freshness_result


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
