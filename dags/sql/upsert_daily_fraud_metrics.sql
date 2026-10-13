	INSERT INTO analytics.daily_fraud_metrics (
		report_date,
		period_start,
		period_end,
		transaction_count,
		evaluated_transaction_count,
		predicted_fraud_count,
		predicted_fraud_rate_percent,
		predicted_fraud_amount,
		generated_at,
		airflow_run_id
	)
	VALUES(
		CAST(%(report_date)s AS DATE),
		CAST(%(period_start)s AS TIMESTAMPTZ),
		CAST(%(period_end)s AS TIMESTAMPTZ),
		%(transaction_count)s,
		%(evaluated_transaction_count)s,
		%(predicted_fraud_count)s,
		%(predicted_fraud_rate_percent)s,
		%(predicted_fraud_amount)s,
		NOW(),
		%(airflow_run_id)s
	)
	ON CONFLICT (report_date)
	DO UPDATE SET
		period_start = EXCLUDED.period_start,
		period_end = EXCLUDED.period_end,
		transaction_count = EXCLUDED.transaction_count,
		evaluated_transaction_count = EXCLUDED.evaluated_transaction_count,
		predicted_fraud_count = EXCLUDED.predicted_fraud_count,
		predicted_fraud_rate_percent = EXCLUDED.predicted_fraud_rate_percent,
		predicted_fraud_amount = EXCLUDED.predicted_fraud_amount,
		generated_at = NOW(),
		airflow_run_id = EXCLUDED.airflow_run_id
		
	RETURNING
		report_date,
		period_start,
		period_end,
		transaction_count,
		evaluated_transaction_count,
		predicted_fraud_count,
		predicted_fraud_rate_percent,
		predicted_fraud_amount,
		generated_at,
		airflow_run_id
;
	
