WITH daily_transactions AS (
	SELECT
		t.transaction_id,
		t.amt,
		p.prediction_id,
		p.fraud_prediction IS TRUE AS is_predicted_fraud

	FROM public.transactions AS t
		LEFT JOIN public.predictions AS p
			ON p.transaction_id = t.transaction_id
	WHERE t.received_at >= CAST(%(period_start)s AS TIMESTAMPTZ)
	  AND t.received_at < CAST(%(period_end)s AS TIMESTAMPTZ)
 )
 
 SELECT 
	COUNT(*) AS transaction_count,
	COUNT(prediction_id) AS evaluated_transaction_count,
	COUNT(*) FILTER (
        WHERE is_predicted_fraud
    ) AS predicted_fraud_count,
	
    COALESCE(
        SUM(amt) FILTER (
            WHERE is_predicted_fraud
        ),
        0
    ) AS predicted_fraud_amount

FROM daily_transactions;
