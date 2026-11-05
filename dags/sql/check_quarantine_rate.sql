-- Payments of the last 24 hours: quarantined (invalid data) and stored.
WITH quarantined_24h AS (
    SELECT COUNT(*) AS quarantined_count
    FROM public.quarantined_payments
    WHERE quarantined_at >= NOW() - INTERVAL '24 hours'
),
stored_24h AS (
    SELECT COUNT(*) AS stored_count
    FROM public.transactions
    WHERE received_at >= NOW() - INTERVAL '24 hours'
)
SELECT
    quarantined_24h.quarantined_count,
    stored_24h.stored_count
FROM quarantined_24h
CROSS JOIN stored_24h;