-- Upsert the daily metrics and report what happened to the row:
--   'created'   : first run for this report_date
--   'updated'   : re-run, at least one metric changed
--   'unchanged' : re-run, identical metrics (only generated_at / airflow_run_id refreshed)
--
-- All CTEs of a single statement share the same snapshot, so "previous"
-- sees the row as it was BEFORE the upsert.
WITH previous AS (
	SELECT
		period_start,
		period_end,
		transaction_count,
		evaluated_transaction_count,
		predicted_fraud_count,
		predicted_fraud_rate_percent,
		predicted_fraud_amount
	FROM analytics.daily_fraud_metrics
	WHERE report_date = CAST(%(report_date)s AS DATE)
),
upserted AS (
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
)
SELECT
	u.report_date,
	u.period_start,
	u.period_end,
	u.transaction_count,
	u.evaluated_transaction_count,
	u.predicted_fraud_count,
	u.predicted_fraud_rate_percent,
	u.predicted_fraud_amount,
	u.generated_at,
	u.airflow_run_id,
	CASE
		WHEN NOT EXISTS (SELECT 1 FROM previous) THEN 'created'
		WHEN (
			SELECT ROW(
				p.period_start,
				p.period_end,
				p.transaction_count,
				p.evaluated_transaction_count,
				p.predicted_fraud_count,
				p.predicted_fraud_rate_percent,
				p.predicted_fraud_amount
			) FROM previous AS p
		) IS NOT DISTINCT FROM ROW(
			u.period_start,
			u.period_end,
			u.transaction_count,
			u.evaluated_transaction_count,
			u.predicted_fraud_count,
			u.predicted_fraud_rate_percent,
			u.predicted_fraud_amount
		) THEN 'unchanged'
		ELSE 'updated'
	END AS persistence_status
FROM upserted AS u
;
