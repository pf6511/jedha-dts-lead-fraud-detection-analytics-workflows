-- Predictions of the last 7 days compared with the ground truth.
SELECT
    COUNT(*) AS evaluated_count,
    COUNT(*) FILTER (WHERE ground_truth = 1) AS actual_fraud_count,
    COUNT(*) FILTER (WHERE fraud_prediction) AS predicted_fraud_count,
    COUNT(*) FILTER (WHERE fraud_prediction AND ground_truth = 1) AS true_positive_count
FROM public.predictions
WHERE prediction_timestamp >= NOW() - INTERVAL '7 days';
