-- Failed payments waiting for a replay, and the age of the oldest one.
-- oldest_pending_hours is NULL when nothing is pending.
SELECT
    COUNT(*) AS pending_count,
    --SELECT EXTRACT(EPOCH FROM INTERVAL '1 day');
    EXTRACT(EPOCH FROM (NOW() - MIN(failed_at))) / 3600 AS oldest_pending_hours
FROM public.failed_payments
WHERE status = 'pending';
