-- One row per check and per run (checked_at defaults to NOW()).
INSERT INTO monitoring.check_results (
    check_name,
    value,
    threshold,
    status,
    details
)
VALUES (
    %(check_name)s,
    %(value)s,
    %(threshold)s,
    %(status)s,
    %(details)s
);
