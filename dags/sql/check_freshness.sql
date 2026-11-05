-- Minutes since the last transaction stored by the processor.
-- NULL when no transaction has been stored yet.
SELECT
    EXTRACT(EPOCH FROM (NOW() - MAX(received_at))) / 60
        AS minutes_since_last_transaction
FROM public.transactions;
